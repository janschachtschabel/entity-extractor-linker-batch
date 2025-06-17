"""Prompt builders for question–answer pair generation."""

from typing import List


def _build_refs_block(references: List[str], lang: str) -> str:
    lines = [(f"({i+1}) {ref}") for i, ref in enumerate(references)]
    header = "### References:" if lang.startswith("en") else "### Referenzen:"
    return header + "\n" + "\n".join(lines) + "\n\n"


def get_system_prompt_qa_en(topic: str, qa_count: int, qa_len: int, references: List[str] | None = None) -> str:
    refs_block = _build_refs_block(references, "en") if references else ""
    return (
        f"{refs_block}Create exactly {qa_count} concise question–answer pairs (QA pairs) about the topic: {topic}.\n"
        f"Requirements:\n"
        f"- Each answer may be at most {qa_len} characters long.\n"
        f"- Use formal academic language.\n"
        f"- Number the pairs (1) … ({qa_count}).\n"
        f"- Output must be strictly JSON in the following form:\n"
        f"  [{{\"question\": \"...\", \"answer\": \"...\"}}, …]\n"
        f"Do NOT wrap the JSON in markdown fences or any explanatory text."
    )


def get_system_prompt_qa_de(topic: str, qa_count: int, qa_len: int, references: List[str] | None = None) -> str:
    refs_block = _build_refs_block(references, "de") if references else ""
    return (
        f"{refs_block}Erstellen Sie genau {qa_count} prägnante Frage–Antwort-Paare (QA-Paare) zum Thema: {topic}.\n"
        f"Vorgaben:\n"
        f"- Jede Antwort darf maximal {qa_len} Zeichen lang sein.\n"
        f"- Verwenden Sie eine formelle, akademische Sprache.\n"
        f"- Nummerieren Sie die Paare (1) … ({qa_count}).\n"
        f"- Geben Sie ausschließlich JSON im folgenden Format aus:\n"
        f"  [{{\"question\": \"...\", \"answer\": \"...\"}}, …]\n"
        f"KEINE Markdown-Codeblöcke oder zusätzlichen Erklärungen."
    )
