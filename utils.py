"""Chunk loader and LLM question generator for the ANAC simulator."""

import json
import random
import time
from dataclasses import dataclass
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Airline → chunk directory mapping
# ---------------------------------------------------------------------------
AIRLINE_TO_DIR: dict[str, str] = {
    "GOL": "oragen",
    "LATAM": "red",
    "Azul": "blue",
}

CHUNKS_ROOT = Path(__file__).resolve().parent / "output" / "chunks"

# ---------------------------------------------------------------------------
# Chunk dataclass
# ---------------------------------------------------------------------------

@dataclass
class Chunk:
    empresa: str
    tema: str
    capitulo: str
    secao: str
    secao_titulo: str
    paginas: str
    fonte: str
    content: str
    filepath: str


# ---------------------------------------------------------------------------
# Chunk loading helpers
# ---------------------------------------------------------------------------

def parse_chunk(filepath: Path) -> Chunk | None:
    """Parse a chunk markdown file. Returns None if content is too short."""
    text = filepath.read_text(encoding="utf-8")

    # Split YAML frontmatter from body
    if not text.startswith("---"):
        return None
    parts = text.split("---", 2)
    if len(parts) < 3:
        return None

    meta = yaml.safe_load(parts[1])
    body = parts[2].strip()

    if len(body) < 150:
        return None

    return Chunk(
        empresa=meta.get("empresa", ""),
        tema=meta.get("tema", ""),
        capitulo=str(meta.get("capitulo", "")),
        secao=str(meta.get("secao", "")),
        secao_titulo=meta.get("secao_titulo", ""),
        paginas=str(meta.get("paginas", "")),
        fonte=meta.get("fonte", ""),
        content=body,
        filepath=str(filepath),
    )


def load_chunks(airline: str) -> list[Chunk]:
    """Load all valid chunks for a given airline."""
    dir_name = AIRLINE_TO_DIR.get(airline)
    if not dir_name:
        return []
    chunk_dir = CHUNKS_ROOT / dir_name
    if not chunk_dir.exists():
        return []

    chunks: list[Chunk] = []
    for md_file in sorted(chunk_dir.glob("*.md")):
        chunk = parse_chunk(md_file)
        if chunk is not None:
            chunks.append(chunk)
    return chunks


def get_topics(chunks: list[Chunk]) -> list[str]:
    """Return sorted unique topics from chunks."""
    return sorted({c.tema for c in chunks if c.tema})


def filter_chunks(chunks: list[Chunk], topic: str) -> list[Chunk]:
    """Filter chunks by topic. If topic is empty, return all."""
    if not topic:
        return chunks
    return [c for c in chunks if c.tema == topic]


def pick_random_chunk(chunks: list[Chunk]) -> Chunk:
    """Pick a random chunk, preferring those with more than 300 chars."""
    long_chunks = [c for c in chunks if len(c.content) > 300]
    pool = long_chunks if long_chunks else chunks
    return random.choice(pool)


# ---------------------------------------------------------------------------
# LLM question generation
# ---------------------------------------------------------------------------

QUESTION_PROMPT = """\
Você é um examinador da ANAC (Agência Nacional de Aviação Civil) especializado \
em provas para comissários de voo.

A partir EXCLUSIVAMENTE do trecho de manual abaixo, crie 1 questão de múltipla \
escolha com 4 alternativas (A, B, C, D). Siga estas regras:

1. A resposta correta deve estar fundamentada no trecho fornecido.
2. Os distratores devem ser plausíveis mas claramente incorretos pelo trecho.
3. Prefira perguntas sobre procedimentos, responsabilidades e normas.
4. A questão deve ter nível compatível com prova da ANAC.
5. Escreva em português brasileiro.
6. Responda APENAS com o JSON abaixo, sem markdown, sem texto extra.

Formato JSON obrigatório:
{{
  "pergunta": "texto da pergunta",
  "alternativas": {{
    "A": "texto alternativa A",
    "B": "texto alternativa B",
    "C": "texto alternativa C",
    "D": "texto alternativa D"
  }},
  "resposta_correta": "A",
  "explicacao": "explicação fundamentada citando o trecho do manual que sustenta a resposta correta, em linguagem técnica formal"
}}

--- TRECHO DO MANUAL ---
{chunk_text}
"""

REQUIRED_KEYS = {"pergunta", "alternativas", "resposta_correta", "explicacao"}

# ---------------------------------------------------------------------------
# Provider definitions
# ---------------------------------------------------------------------------

PROVIDERS: dict[str, dict] = {
    "DeepSeek": {
        "env_key": "DEEPSEEK_API_KEY",
        "models": [
            "deepseek-chat",
            "deepseek-reasoner",
        ],
    },
    "Anthropic": {
        "env_key": "ANTHROPIC_API_KEY",
        "models": [
            "claude-sonnet-4-5-20250929",
            "claude-haiku-4-5-20251001",
        ],
    },
    "OpenAI": {
        "env_key": "OPENAI_API_KEY",
        "models": [
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-4.1",
            "gpt-4.1-mini",
            "gpt-4.1-nano",
        ],
    },
    "Google Gemini": {
        "env_key": "GEMINI_API_KEY",
        "models": [
            "gemini-2.0-flash",
            "gemini-2.5-flash-preview-05-20",
            "gemini-2.5-pro-preview-05-06",
        ],
    },
}


def _call_anthropic(api_key: str, model: str, prompt: str) -> str:
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=model,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


def _call_openai(api_key: str, model: str, prompt: str) -> str:
    import openai
    client = openai.OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content.strip()


def _call_gemini(api_key: str, model: str, prompt: str) -> str:
    from google import genai
    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model=model,
        contents=prompt,
    )
    return response.text.strip()


def _call_deepseek(api_key: str, model: str, prompt: str) -> str:
    import openai
    client = openai.OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    response = client.chat.completions.create(
        model=model,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content.strip()


_PROVIDER_CALLERS = {
    "Anthropic": _call_anthropic,
    "OpenAI": _call_openai,
    "Google Gemini": _call_gemini,
    "DeepSeek": _call_deepseek,
}


def _parse_response(raw: str) -> dict:
    """Parse and validate the JSON response from any LLM."""
    # Strip possible markdown fences
    if raw.startswith("```"):
        lines = raw.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        raw = "\n".join(lines)

    data = json.loads(raw)

    missing = REQUIRED_KEYS - set(data.keys())
    if missing:
        raise ValueError(f"Missing keys in LLM response: {missing}")

    if not isinstance(data["alternativas"], dict) or len(data["alternativas"]) != 4:
        raise ValueError("alternativas must be a dict with exactly 4 entries")

    if data["resposta_correta"] not in data["alternativas"]:
        raise ValueError(
            f"resposta_correta '{data['resposta_correta']}' not in alternativas"
        )

    return data


def generate_question(chunk: Chunk, provider: str, api_key: str, model: str) -> dict:
    """Generate a multiple-choice question from a chunk using the chosen provider.

    Returns a dict with keys: pergunta, alternativas, resposta_correta, explicacao.
    Raises ValueError on parse/validation failure.
    """
    truncated = chunk.content[:4000]
    prompt = QUESTION_PROMPT.format(chunk_text=truncated)

    caller = _PROVIDER_CALLERS[provider]
    raw = caller(api_key, model, prompt)

    return _parse_response(raw)


# ---------------------------------------------------------------------------
# Media generation (Imagen / Veo) — requires GEMINI_API_KEY
# ---------------------------------------------------------------------------

IMAGE_PROMPT_TEMPLATE = (
    "Ilustração educacional profissional para treinamento de comissários de voo. "
    "Tema: {tema}. Contexto: {pergunta}. "
    "Estilo: ilustração técnica didática de aviação civil, cores claras, sem texto."
)

VIDEO_PROMPT_TEMPLATE = (
    "Cena curta educacional de aviação civil mostrando o interior de um avião comercial. "
    "Tema: {tema}. "
    "Estilo: filmagem realista, iluminação suave, sem pessoas visíveis."
)


def _get_gemini_client(gemini_api_key: str):
    from google import genai
    return genai.Client(api_key=gemini_api_key)


def generate_image(
    question_text: str,
    tema: str,
    gemini_api_key: str,
    model: str = "imagen-3.0-generate-002",
) -> bytes | None:
    """Generate an illustrative image using Imagen. Returns JPEG bytes or None."""
    from google.genai import types

    client = _get_gemini_client(gemini_api_key)
    prompt = IMAGE_PROMPT_TEMPLATE.format(tema=tema, pergunta=question_text[:200])

    response = client.models.generate_images(
        model=model,
        prompt=prompt,
        config=types.GenerateImagesConfig(
            number_of_images=1,
            output_mime_type="image/jpeg",
        ),
    )

    if response.generated_images:
        return response.generated_images[0].image.image_bytes
    return None


def generate_video(
    tema: str,
    gemini_api_key: str,
    model: str = "veo-3.0-fast-generate-001",
    duration: int = 5,
    poll_interval: int = 15,
    max_wait: int = 300,
) -> bytes | None:
    """Generate a short video using Veo. Returns MP4 bytes or None.

    This is a long-running operation — polls until done or max_wait is reached.
    """
    from google.genai import types

    client = _get_gemini_client(gemini_api_key)
    prompt = VIDEO_PROMPT_TEMPLATE.format(tema=tema)

    operation = client.models.generate_videos(
        model=model,
        prompt=prompt,
        config=types.GenerateVideosConfig(
            person_generation="dont_allow",
            aspect_ratio="16:9",
            number_of_videos=1,
            duration_seconds=duration,
        ),
    )

    elapsed = 0
    while not operation.done and elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval
        operation = client.operations.get(operation)

    if not operation.done:
        raise TimeoutError(f"Video generation exceeded {max_wait}s timeout")

    if operation.response and operation.response.generated_videos:
        video = operation.response.generated_videos[0].video
        client.files.download(file=video)
        return video.video_bytes
    return None
