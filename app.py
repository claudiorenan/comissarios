"""Simulado ANAC — Comissários de Voo (Streamlit app)."""

import os

import streamlit as st
from dotenv import load_dotenv

from utils import (
    AIRLINE_TO_DIR,
    PROVIDERS,
    filter_chunks,
    generate_image,
    generate_question,
    generate_video,
    get_topics,
    load_chunks,
    pick_random_chunk,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Simulado ANAC", page_icon="✈️", layout="centered")

# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------
_defaults = {
    "chunks": None,
    "airline": None,
    "current_question": None,
    "current_chunk": None,
    "answered": False,
    "selected_answer": None,
    "score_correct": 0,
    "score_total": 0,
    "topic_filter": "",
    "current_image": None,
    "current_video": None,
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("⚙️ Configurações")

    provider = st.selectbox("Provedor", options=list(PROVIDERS.keys()))
    prov_info = PROVIDERS[provider]

    env_key = os.getenv(prov_info["env_key"], "")
    api_key = st.text_input(
        f"API Key ({provider})",
        value=env_key,
        type="password",
        help=f"Preencha aqui ou defina {prov_info['env_key']} no .env",
    )

    model = st.selectbox(
        "Modelo",
        options=prov_info["models"],
        index=0,
    )

    st.divider()
    st.subheader("🎨 Mídia (Google AI)")

    enable_image = st.checkbox("Gerar imagem (Imagen)", value=False)
    enable_video = st.checkbox("Gerar vídeo (Veo 3)", value=False)

    # Gemini key for media — reuse if provider is already Google Gemini
    if enable_image or enable_video:
        if provider == "Google Gemini":
            gemini_key = api_key
        else:
            gemini_env = os.getenv("GEMINI_API_KEY", "")
            gemini_key = st.text_input(
                "GEMINI_API_KEY (para mídia)",
                value=gemini_env,
                type="password",
                help="Necessária para Imagen / Veo, mesmo usando outro provedor para questões.",
            )

        imagen_model = st.selectbox(
            "Modelo Imagen",
            options=["imagen-3.0-generate-002", "imagen-4.0-generate-001"],
            index=0,
        ) if enable_image else None

        veo_model = st.selectbox(
            "Modelo Veo",
            options=["veo-3.0-fast-generate-001", "veo-3.1-generate-preview"],
            index=0,
        ) if enable_video else None
    else:
        gemini_key = ""
        imagen_model = None
        veo_model = None

    st.divider()
    st.subheader("📊 Placar")
    correct = st.session_state["score_correct"]
    total = st.session_state["score_total"]
    if total > 0:
        pct = correct / total * 100
        color = "green" if pct >= 70 else "orange" if pct >= 50 else "red"
        st.markdown(
            f"**{correct}/{total}** — "
            f"<span style='color:{color};font-weight:bold'>{pct:.0f}%</span>",
            unsafe_allow_html=True,
        )
    else:
        st.write("Nenhuma questão respondida.")

    if st.button("Zerar placar"):
        st.session_state["score_correct"] = 0
        st.session_state["score_total"] = 0
        st.rerun()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _check_api_key():
    """Ensure an API key is set, otherwise stop."""
    if not api_key:
        st.error("Informe a API Key na sidebar ou no arquivo .env")
        st.stop()


def _reset_question():
    st.session_state["current_question"] = None
    st.session_state["current_chunk"] = None
    st.session_state["answered"] = False
    st.session_state["selected_answer"] = None
    st.session_state["current_image"] = None
    st.session_state["current_video"] = None


def _change_airline():
    st.session_state["chunks"] = None
    st.session_state["airline"] = None
    st.session_state["topic_filter"] = ""
    _reset_question()


# ---------------------------------------------------------------------------
# STATE 1 — Setup: choose airline
# ---------------------------------------------------------------------------
if st.session_state["chunks"] is None:
    st.title("✈️ Simulado ANAC — Comissários de Voo")
    st.write("Selecione a companhia aérea para carregar o manual.")

    airline = st.selectbox("Companhia", options=list(AIRLINE_TO_DIR.keys()))

    if st.button("Carregar manual", type="primary"):
        with st.spinner(f"Carregando chunks de {airline}..."):
            chunks = load_chunks(airline)
        if not chunks:
            st.error(f"Nenhum chunk encontrado para {airline}.")
        else:
            st.session_state["chunks"] = chunks
            st.session_state["airline"] = airline
            st.success(f"{len(chunks)} trechos carregados.")
            st.rerun()

    st.stop()

# ---------------------------------------------------------------------------
# From here on, chunks are loaded.
# ---------------------------------------------------------------------------
chunks = st.session_state["chunks"]
airline = st.session_state["airline"]

st.title(f"✈️ Simulado — {airline}")

# Topic filter
topics = get_topics(chunks)
topic_options = ["Todos"] + topics
selected_topic = st.selectbox("Filtrar por tema", options=topic_options)
effective_topic = "" if selected_topic == "Todos" else selected_topic
st.session_state["topic_filter"] = effective_topic

col1, col2 = st.columns(2)
with col1:
    if st.button("Trocar companhia"):
        _change_airline()
        st.rerun()

# ---------------------------------------------------------------------------
# STATE 2 — Generate question
# ---------------------------------------------------------------------------
if st.session_state["current_question"] is None:
    filtered = filter_chunks(chunks, effective_topic)
    if not filtered:
        st.warning("Nenhum trecho disponível para este tema.")
        st.stop()

    with col2:
        generate = st.button("Gerar questão", type="primary")

    if not generate:
        st.info("Clique em **Gerar questão** para começar.")
        st.stop()

    _check_api_key()
    chunk = pick_random_chunk(filtered)

    with st.spinner("Gerando questão com IA..."):
        try:
            question = generate_question(chunk, provider, api_key, model)
        except Exception as e:
            st.error(f"Erro ao gerar questão: {e}")
            st.stop()

    st.session_state["current_question"] = question
    st.session_state["current_chunk"] = chunk
    st.session_state["answered"] = False
    st.session_state["selected_answer"] = None

    # --- Media generation (optional) ---
    tema = chunk.tema or "aviação civil"

    if enable_image and gemini_key:
        with st.spinner("Gerando imagem ilustrativa..."):
            try:
                img_bytes = generate_image(
                    question["pergunta"], tema, gemini_key, imagen_model
                )
                st.session_state["current_image"] = img_bytes
            except Exception as e:
                st.warning(f"Imagem não gerada: {e}")

    if enable_video and gemini_key:
        with st.spinner("Gerando vídeo (pode levar alguns minutos)..."):
            try:
                vid_bytes = generate_video(tema, gemini_key, veo_model)
                st.session_state["current_video"] = vid_bytes
            except Exception as e:
                st.warning(f"Vídeo não gerado: {e}")

    st.rerun()

# ---------------------------------------------------------------------------
# Question is loaded — show it.
# ---------------------------------------------------------------------------
question = st.session_state["current_question"]
chunk = st.session_state["current_chunk"]
alts = question["alternativas"]


def _show_media():
    """Render image and/or video if available."""
    img = st.session_state.get("current_image")
    vid = st.session_state.get("current_video")
    if img or vid:
        cols = st.columns(2 if (img and vid) else 1)
        idx = 0
        if img:
            with cols[idx]:
                st.image(img, caption="Ilustração gerada por IA (Imagen)", use_container_width=True)
            idx += 1
        if vid:
            with cols[idx]:
                st.video(vid, format="video/mp4")
                st.caption("Vídeo gerado por IA (Veo 3)")


# ---------------------------------------------------------------------------
# STATE 3 — Show question, await answer
# ---------------------------------------------------------------------------
if not st.session_state["answered"]:
    st.subheader(question["pergunta"])
    _show_media()

    options = [f"{k}) {v}" for k, v in alts.items()]
    choice = st.radio("Escolha uma alternativa:", options, index=None, label_visibility="collapsed")

    if st.button("Confirmar resposta", type="primary"):
        if choice is None:
            st.warning("Selecione uma alternativa antes de confirmar.")
        else:
            letter = choice[0]  # "A) ..." → "A"
            st.session_state["selected_answer"] = letter
            st.session_state["answered"] = True

            # Update score
            st.session_state["score_total"] += 1
            if letter == question["resposta_correta"]:
                st.session_state["score_correct"] += 1

            st.rerun()

    st.stop()

# ---------------------------------------------------------------------------
# STATE 4 — Show result
# ---------------------------------------------------------------------------
selected = st.session_state["selected_answer"]
correct_letter = question["resposta_correta"]
is_correct = selected == correct_letter

st.subheader(question["pergunta"])
_show_media()

# Show alternatives with color coding
for letter, text in alts.items():
    if letter == correct_letter:
        st.markdown(
            f"<div style='padding:8px 12px;border-radius:6px;background:#d4edda;color:#155724;margin-bottom:4px'>"
            f"<strong>{letter})</strong> {text} ✅</div>",
            unsafe_allow_html=True,
        )
    elif letter == selected and not is_correct:
        st.markdown(
            f"<div style='padding:8px 12px;border-radius:6px;background:#f8d7da;color:#721c24;margin-bottom:4px'>"
            f"<strong>{letter})</strong> {text} ❌</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f"<div style='padding:8px 12px;border-radius:6px;background:#f8f9fa;color:#333;margin-bottom:4px'>"
            f"<strong>{letter})</strong> {text}</div>",
            unsafe_allow_html=True,
        )

st.write("")

if is_correct:
    st.success("🎉 Parabéns! Resposta correta!")
else:
    st.error(f"Resposta correta: **{correct_letter}) {alts[correct_letter]}**")

# Explanation + reference
with st.expander("📖 Explicação e referência"):
    st.write(question["explicacao"])
    st.divider()
    st.caption(
        f"**Fonte:** {chunk.fonte}  \n"
        f"**Capítulo:** {chunk.capitulo}  \n"
        f"**Seção:** {chunk.secao_titulo}  \n"
        f"**Páginas:** {chunk.paginas}"
    )

if st.button("Próxima questão ➡️", type="primary"):
    _reset_question()
    st.rerun()
