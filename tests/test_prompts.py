"""Tests for MCP prompt templates."""

import pytest
from jcodemunch_mcp.server import list_prompts, get_prompt


@pytest.mark.asyncio
async def test_list_prompts_returns_five():
    """list_prompts returns exactly 5 prompts."""
    prompts = await list_prompts()
    assert len(prompts) == 5


@pytest.mark.asyncio
async def test_list_prompts_names():
    """All expected prompt names are present."""
    prompts = await list_prompts()
    names = {p.name for p in prompts}
    assert names == {"workflow", "explore", "assess", "triage", "trace"}


@pytest.mark.asyncio
async def test_list_prompts_descriptions_non_empty():
    """Every prompt has a non-empty description."""
    prompts = await list_prompts()
    for p in prompts:
        assert p.description, f"Prompt {p.name!r} has empty description"


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["workflow", "explore", "assess", "triage", "trace"])
async def test_get_prompt_returns_non_empty_text(name):
    """Each prompt returns a GetPromptResult with non-empty text."""
    result = await get_prompt(name)
    assert result.description
    assert result.messages
    text = result.messages[0].content.text
    assert len(text) > 50, f"Prompt {name!r} text is too short: {len(text)} chars"


@pytest.mark.asyncio
async def test_get_prompt_unknown_raises():
    """Requesting a nonexistent prompt raises ValueError."""
    with pytest.raises(ValueError, match="Unknown prompt"):
        await get_prompt("nonexistent")
