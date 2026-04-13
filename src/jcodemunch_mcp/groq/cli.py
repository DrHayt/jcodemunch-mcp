"""gcm — Codebase Q&A powered by jCodeMunch + Groq.

Usage:
    gcm "how does authentication work?" --repo pallets/flask
    gcm "where are the API routes?"                          # current directory
    gcm --chat --repo facebook/react                         # interactive mode
"""

import argparse
import os
import sys
import time
from typing import Optional

from .config import GcmConfig, DEFAULT_MODEL, FAST_MODEL


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="gcm",
        description="Ask any question about any codebase. Powered by jCodeMunch + Groq.",
    )
    p.add_argument("question", nargs="?", help="Question to ask about the codebase")
    p.add_argument("--repo", "-r", help="GitHub repo (owner/name) or local path (default: current directory)")
    p.add_argument("--model", "-m", default=DEFAULT_MODEL, help=f"Groq model (default: {DEFAULT_MODEL})")
    p.add_argument("--fast", action="store_const", const=FAST_MODEL, dest="model", help=f"Use fast model ({FAST_MODEL})")
    p.add_argument("--budget", "-b", type=int, default=8000, help="Token budget for context retrieval (default: 8000)")
    p.add_argument("--chat", "-c", action="store_true", help="Interactive multi-turn chat mode")
    p.add_argument("--no-stream", action="store_true", help="Disable streaming output")
    p.add_argument("--verbose", "-v", action="store_true", help="Show timing and retrieval details")
    p.add_argument("--version", action="store_true", help="Show version and exit")
    return p


def _print_answer_streaming(cfg: GcmConfig, context: str, question: str, history: Optional[list[dict]] = None) -> str:
    """Stream answer to stdout and return full text."""
    from .inference import ask_stream

    full = []
    for token in ask_stream(cfg, context, question, history):
        sys.stdout.write(token)
        sys.stdout.flush()
        full.append(token)
    sys.stdout.write("\n")
    return "".join(full)


def _print_answer_batch(cfg: GcmConfig, context: str, question: str, history: Optional[list[dict]] = None) -> str:
    """Get full answer then print. Returns the text."""
    from .inference import ask

    answer = ask(cfg, context, question, history)
    print(answer)
    return answer


def _run_single(cfg: GcmConfig, repo_id: str, question: str, stream: bool, verbose: bool) -> None:
    """Answer a single question."""
    from .retriever import retrieve_context

    t0 = time.perf_counter()
    context, raw = retrieve_context(repo_id, question, cfg.token_budget, cfg.storage_path)
    t_retrieve = time.perf_counter() - t0

    if "error" in raw:
        print(f"Retrieval error: {raw['error']}", file=sys.stderr)
        sys.exit(1)

    if verbose:
        n_items = len(raw.get("context_items", []))
        tokens_used = raw.get("tokens_used", "?")
        print(f"[retrieval] {n_items} symbols, {tokens_used} tokens in {t_retrieve:.2f}s", file=sys.stderr)

    t1 = time.perf_counter()
    if stream:
        _print_answer_streaming(cfg, context, question)
    else:
        _print_answer_batch(cfg, context, question)

    if verbose:
        t_infer = time.perf_counter() - t1
        t_total = time.perf_counter() - t0
        print(f"[inference] {t_infer:.2f}s | [total] {t_total:.2f}s", file=sys.stderr)


def _run_chat(cfg: GcmConfig, repo_id: str, stream: bool, verbose: bool) -> None:
    """Interactive multi-turn chat loop."""
    from .retriever import retrieve_context

    history: list[dict] = []
    print(f"Chat with {repo_id} (type 'exit' or Ctrl+C to quit)\n")

    while True:
        try:
            question = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not question:
            continue
        if question.lower() in ("exit", "quit", "q"):
            print("Bye!")
            break

        t0 = time.perf_counter()
        context, raw = retrieve_context(repo_id, question, cfg.token_budget, cfg.storage_path)

        if "error" in raw:
            print(f"Retrieval error: {raw['error']}", file=sys.stderr)
            continue

        if verbose:
            n_items = len(raw.get("context_items", []))
            t_retrieve = time.perf_counter() - t0
            print(f"[retrieval] {n_items} symbols in {t_retrieve:.2f}s", file=sys.stderr)

        if stream:
            answer = _print_answer_streaming(cfg, context, question, history)
        else:
            answer = _print_answer_batch(cfg, context, question, history)

        # Append to history for multi-turn context
        history.append({"role": "user", "content": question})
        history.append({"role": "assistant", "content": answer})

        if verbose:
            t_total = time.perf_counter() - t0
            print(f"[total] {t_total:.2f}s", file=sys.stderr)

        print()  # blank line between turns


def main(argv: Optional[list[str]] = None) -> None:
    """CLI entrypoint for gcm."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.version:
        try:
            from importlib.metadata import version as pkg_version
            v = pkg_version("jcodemunch-mcp")
        except Exception:
            v = "unknown"
        print(f"gcm (jcodemunch-mcp {v})")
        return

    # Build config
    cfg = GcmConfig(model=args.model, token_budget=args.budget)
    err = cfg.validate()
    if err:
        print(f"Error: {err}", file=sys.stderr)
        sys.exit(1)

    # Resolve repo
    repo = args.repo or "."
    if repo == ".":
        repo = os.getcwd()

    # Ensure indexed
    from .retriever import ensure_indexed

    if args.verbose:
        print(f"[repo] {repo}", file=sys.stderr)

    repo_id, idx_err = ensure_indexed(repo, cfg.storage_path, cfg.github_token, verbose=args.verbose)
    if idx_err:
        print(f"Error: {idx_err}", file=sys.stderr)
        sys.exit(1)

    if args.verbose:
        print(f"[indexed] {repo_id}", file=sys.stderr)

    # Route to chat or single-question mode
    stream = not args.no_stream

    if args.chat:
        _run_chat(cfg, repo_id, stream, args.verbose)
    elif args.question:
        _run_single(cfg, repo_id, args.question, stream, args.verbose)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
