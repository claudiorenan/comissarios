"""Simulado ANAC — Comissarios de Voo (Streamlit app)."""

import base64
import os
from pathlib import Path

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
# Theme → background image mapping
# ---------------------------------------------------------------------------
BACKGROUNDS_DIR = Path(__file__).resolve().parent / "assets" / "backgrounds"

_THEME_TO_BG = {
    "Safety": "safety.jpg",
    "Security": "security.jpg",
    "Equipamentos de Emerg\u00eancia": "emergency.jpg",
    "PROCEDIMENTOS DE EMERG\u00caNCIA": "emergency.jpg",
    "Procedimentos de Emerg\u00eancia": "emergency.jpg",
    "Procedimentos Anormais &": "emergency.jpg",
    "PRIMEIROS SOCORROS": "first_aid.jpg",
    "Primeiros Socorros": "first_aid.jpg",
    "Sa\u00fade Aeroespacial e Primeiros Socorros": "first_aid.jpg",
    "Airbus 320 Family": "aircraft.jpg",
    "Boeing 777": "aircraft.jpg",
    "Boeing 787": "aircraft.jpg",
    "PROCEDIMENTOS OPERACIONAIS": "operations.jpg",
    "Standard Operating Procedures": "operations.jpg",
    "Padroniza\u00e7\u00e3o Operacional": "operations.jpg",
    "Normas e Procedimentos": "operations.jpg",
    "SOBREVIV\u00caNCIA": "survival.jpg",
    "Sobreviv\u00eancia": "survival.jpg",
    "Sobreviv\u00eancia na Selva, Mar, Deserto e Gelo": "survival.jpg",
    "Tripula\u00e7\u00e3o de Cabine": "crew.jpg",
    "APRESENTA\u00c7\u00c3O": "crew.jpg",
    "Generalidades": "crew.jpg",
    "Organiza\u00e7\u00e3o da Empresa": "crew.jpg",
    "COMPOSI\u00c7\u00c3O DO MANUAL": "crew.jpg",
    "REGULAMENTOS AERON\u00c1UTICOS": "regulations.jpg",
    "Artigos Perigosos": "dangerous_goods.jpg",
    "FORMUL\u00c1RIOS": "operations.jpg",
    "GLOSS\u00c1RIO": "operations.jpg",
    "\u00cdndice Geral": "default.jpg",
}


@st.cache_data
def _load_bg_b64(filename: str) -> str | None:
    """Load a background image as base64 for CSS embedding."""
    filepath = BACKGROUNDS_DIR / filename
    if not filepath.exists():
        return None
    data = filepath.read_bytes()
    return base64.b64encode(data).decode()


def _get_bg_for_topic(topic: str) -> str | None:
    """Return base64 background for a topic, or default."""
    filename = _THEME_TO_BG.get(topic, "default.jpg")
    return _load_bg_b64(filename)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Simulado ANAC", page_icon="✈️", layout="centered")

# ---------------------------------------------------------------------------
# Aviation Theme CSS
# ---------------------------------------------------------------------------
AVIATION_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

/* Global theme */
.stApp {
    background: linear-gradient(180deg, #0a1628 0%, #132238 40%, #1a2d4a 100%);
}

/* Hide default header */
header[data-testid="stHeader"] {
    background: rgba(10, 22, 40, 0.95);
    backdrop-filter: blur(10px);
}

/* Hide toolbar buttons (Share, GitHub, star, edit pencil) */
[data-testid="stToolbar"] {
    display: none !important;
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0d1b2e 0%, #162a45 100%);
    border-right: 1px solid rgba(212, 175, 55, 0.2);
}

section[data-testid="stSidebar"] .stMarkdown {
    color: #c8d6e5;
}

/* Main content text */
.stApp .stMarkdown, .stApp p, .stApp label, .stApp span {
    font-family: 'Inter', sans-serif;
}

/* Hero banner */
.hero-banner {
    background: linear-gradient(135deg, #0d1f3c 0%, #1a3a5c 50%, #0d1f3c 100%);
    border: 1px solid rgba(212, 175, 55, 0.3);
    border-radius: 16px;
    padding: 40px 30px;
    text-align: center;
    margin-bottom: 30px;
    position: relative;
    overflow: hidden;
}

.hero-banner::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: linear-gradient(90deg, transparent, #d4af37, transparent);
}

.hero-banner::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 1px;
    background: linear-gradient(90deg, transparent, rgba(212,175,55,0.3), transparent);
}

.hero-title {
    font-family: 'Inter', sans-serif;
    font-size: 2.4em;
    font-weight: 800;
    color: #ffffff;
    margin: 0;
    letter-spacing: -0.5px;
    text-shadow: 0 2px 20px rgba(0,0,0,0.5);
}

.hero-subtitle {
    font-family: 'Inter', sans-serif;
    font-size: 1.1em;
    color: #d4af37;
    margin-top: 8px;
    font-weight: 500;
    letter-spacing: 3px;
    text-transform: uppercase;
}

.hero-wings {
    font-size: 3em;
    margin-bottom: 10px;
    filter: drop-shadow(0 2px 8px rgba(212,175,55,0.4));
}

/* Card container */
.aviation-card {
    background: linear-gradient(145deg, rgba(18, 35, 60, 0.9), rgba(13, 25, 45, 0.95));
    border: 1px solid rgba(212, 175, 55, 0.15);
    border-radius: 12px;
    padding: 24px;
    margin-bottom: 16px;
    backdrop-filter: blur(10px);
    box-shadow: 0 4px 20px rgba(0,0,0,0.3);
}

.aviation-card:hover {
    border-color: rgba(212, 175, 55, 0.35);
    box-shadow: 0 6px 30px rgba(0,0,0,0.4);
}

.card-title {
    font-family: 'Inter', sans-serif;
    font-size: 1.1em;
    font-weight: 700;
    color: #d4af37;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 8px;
}

/* Airline cards */
.airline-card {
    background: linear-gradient(145deg, rgba(18, 35, 60, 0.8), rgba(13, 25, 45, 0.9));
    border: 2px solid rgba(255,255,255,0.08);
    border-radius: 16px;
    padding: 30px 20px;
    text-align: center;
    transition: all 0.3s ease;
    cursor: pointer;
}

.airline-card:hover {
    border-color: rgba(212, 175, 55, 0.5);
    transform: translateY(-2px);
    box-shadow: 0 8px 30px rgba(0,0,0,0.4);
}

.airline-logo {
    font-size: 2.5em;
    margin-bottom: 8px;
}

.airline-name {
    font-family: 'Inter', sans-serif;
    font-size: 1.3em;
    font-weight: 700;
    color: #ffffff;
}

.airline-color-gol { border-top: 3px solid #FF6600; }
.airline-color-latam { border-top: 3px solid #E4002B; }
.airline-color-azul { border-top: 3px solid #0033A0; }

/* Question card */
.question-card {
    background: linear-gradient(145deg, rgba(20, 40, 70, 0.95), rgba(15, 30, 55, 0.98));
    border: 1px solid rgba(212, 175, 55, 0.2);
    border-radius: 16px;
    padding: 28px;
    margin: 20px 0;
    box-shadow: 0 8px 32px rgba(0,0,0,0.3);
}

.question-text {
    font-family: 'Inter', sans-serif;
    font-size: 1.15em;
    font-weight: 600;
    color: #e8edf3;
    line-height: 1.6;
    margin-bottom: 4px;
}

/* Answer options */
.answer-correct {
    padding: 12px 16px;
    border-radius: 10px;
    background: linear-gradient(135deg, rgba(34, 197, 94, 0.15), rgba(34, 197, 94, 0.08));
    border: 1px solid rgba(34, 197, 94, 0.4);
    color: #86efac;
    margin-bottom: 8px;
    font-family: 'Inter', sans-serif;
    font-weight: 500;
}

.answer-wrong {
    padding: 12px 16px;
    border-radius: 10px;
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.15), rgba(239, 68, 68, 0.08));
    border: 1px solid rgba(239, 68, 68, 0.4);
    color: #fca5a5;
    margin-bottom: 8px;
    font-family: 'Inter', sans-serif;
    font-weight: 500;
}

.answer-neutral {
    padding: 12px 16px;
    border-radius: 10px;
    background: rgba(255, 255, 255, 0.04);
    border: 1px solid rgba(255, 255, 255, 0.08);
    color: #94a3b8;
    margin-bottom: 8px;
    font-family: 'Inter', sans-serif;
}

/* Score display */
.score-display {
    background: linear-gradient(135deg, rgba(212, 175, 55, 0.1), rgba(212, 175, 55, 0.05));
    border: 1px solid rgba(212, 175, 55, 0.25);
    border-radius: 12px;
    padding: 16px;
    text-align: center;
}

.score-number {
    font-family: 'Inter', sans-serif;
    font-size: 2em;
    font-weight: 800;
    color: #d4af37;
}

.score-label {
    font-family: 'Inter', sans-serif;
    font-size: 0.85em;
    color: #8899aa;
    text-transform: uppercase;
    letter-spacing: 1px;
}

/* Status indicator */
.status-connected {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(34, 197, 94, 0.1);
    border: 1px solid rgba(34, 197, 94, 0.3);
    border-radius: 20px;
    padding: 4px 14px;
    font-size: 0.85em;
    color: #86efac;
}

/* Cockpit divider */
.cockpit-divider {
    height: 1px;
    background: linear-gradient(90deg, transparent, rgba(212,175,55,0.3), transparent);
    margin: 20px 0;
}

/* Buttons */
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #d4af37, #b8962e) !important;
    color: #0a1628 !important;
    font-weight: 700 !important;
    border: none !important;
    border-radius: 10px !important;
    padding: 12px 24px !important;
    font-family: 'Inter', sans-serif !important;
    letter-spacing: 0.5px !important;
    transition: all 0.3s ease !important;
}

.stButton > button[kind="primary"]:hover {
    background: linear-gradient(135deg, #e4bf47, #c8a63e) !important;
    box-shadow: 0 4px 20px rgba(212, 175, 55, 0.3) !important;
}

.stButton > button[kind="secondary"] {
    background: rgba(255,255,255,0.05) !important;
    color: #c8d6e5 !important;
    border: 1px solid rgba(212, 175, 55, 0.2) !important;
    border-radius: 10px !important;
    font-family: 'Inter', sans-serif !important;
}

/* Input fields */
.stTextInput > div > div > input {
    background: rgba(255,255,255,0.05) !important;
    border: 1px solid rgba(212, 175, 55, 0.2) !important;
    border-radius: 8px !important;
    color: #e8edf3 !important;
    font-family: 'Inter', sans-serif !important;
}

.stTextInput > div > div > input:focus {
    border-color: rgba(212, 175, 55, 0.5) !important;
    box-shadow: 0 0 0 1px rgba(212, 175, 55, 0.2) !important;
}

/* Select boxes */
.stSelectbox > div > div {
    background: rgba(255,255,255,0.05) !important;
    border: 1px solid rgba(212, 175, 55, 0.2) !important;
    border-radius: 8px !important;
}

/* Radio buttons */
.stRadio > div {
    background: transparent !important;
}

/* Expander */
.streamlit-expanderHeader {
    background: rgba(212, 175, 55, 0.08) !important;
    border: 1px solid rgba(212, 175, 55, 0.15) !important;
    border-radius: 10px !important;
    color: #d4af37 !important;
    font-family: 'Inter', sans-serif !important;
}

/* Footer badge */
.footer-badge {
    text-align: center;
    padding: 20px;
    margin-top: 40px;
    border-top: 1px solid rgba(212, 175, 55, 0.1);
}

.footer-text {
    font-family: 'Inter', sans-serif;
    font-size: 0.75em;
    color: #4a5568;
    letter-spacing: 1px;
}
</style>
"""

st.markdown(AVIATION_CSS, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------
_defaults = {
    "authenticated": False,
    "api_key": "",
    "provider": "",
    "model": "",
    "api_tested": False,
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
# LOGIN GATE — password + API key + test
# ---------------------------------------------------------------------------
APP_PASSWORD = "12345678"

if not st.session_state["authenticated"]:

    st.markdown("""
    <div class="hero-banner">
        <div class="hero-wings">✈️</div>
        <p class="hero-title">Simulado ANAC</p>
        <p class="hero-subtitle">Comissarios de Voo</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="aviation-card"><div class="card-title">🔐 Acesso ao Sistema</div>', unsafe_allow_html=True)
    password = st.text_input("Senha de acesso", type="password", placeholder="Digite a senha")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="cockpit-divider"></div>', unsafe_allow_html=True)

    # Default provider/model/key — login só pede senha
    _default_provider = "DeepSeek"
    _default_model = PROVIDERS[_default_provider]["models"][0]
    _env_key_name = PROVIDERS[_default_provider]["env_key"]
    _server_api_key = os.getenv(_env_key_name, "")
    if not _server_api_key:
        try:
            _server_api_key = st.secrets[_env_key_name]
        except (KeyError, FileNotFoundError):
            _server_api_key = ""

    if st.button("Entrar", type="primary", use_container_width=True):
        if password != APP_PASSWORD:
            st.error("Senha incorreta.")
        else:
            st.session_state["authenticated"] = True
            st.session_state["api_key"] = _server_api_key  # pode ser "" — será pedida depois
            st.session_state["provider"] = _default_provider
            st.session_state["model"] = _default_model
            st.rerun()

    st.markdown("""
    <div class="footer-badge">
        <p class="footer-text">SIMULADO ANAC &bull; PREPARATORIO PARA COMISSARIOS DE VOO</p>
    </div>
    """, unsafe_allow_html=True)

    st.stop()

# ---------------------------------------------------------------------------
# STEP 2 — Choose airline
# ---------------------------------------------------------------------------
if st.session_state["chunks"] is None:

    st.markdown("""
    <div class="hero-banner">
        <div class="hero-wings">🛫</div>
        <p class="hero-title">Escolha sua Companhia</p>
        <p class="hero-subtitle">Selecione o manual de treinamento</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(
        f'<div style="text-align:center;margin-bottom:20px">'
        f'<div class="status-connected">'
        f'<span style="font-size:14px">●</span> '
        f'{st.session_state["provider"]} — {st.session_state["model"]}'
        f'</div></div>',
        unsafe_allow_html=True,
    )

    # Airline cards
    cols = st.columns(3)

    airline_info = {
        "GOL": {"icon": "🟠", "color": "gol", "desc": "Linhas Aereas Inteligentes"},
        "LATAM": {"icon": "🔴", "color": "latam", "desc": "LATAM Airlines Brasil"},
        "Azul": {"icon": "🔵", "color": "azul", "desc": "Azul Linhas Aereas"},
    }

    for i, (name, info) in enumerate(airline_info.items()):
        with cols[i]:
            st.markdown(f"""
            <div class="airline-card airline-color-{info['color']}">
                <div class="airline-logo">{info['icon']}</div>
                <div class="airline-name">{name}</div>
                <div style="color:#8899aa;font-size:0.8em;margin-top:4px">{info['desc']}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown('<div class="cockpit-divider"></div>', unsafe_allow_html=True)

    airline = st.selectbox("Companhia Aerea", options=list(AIRLINE_TO_DIR.keys()), key="login_airline")

    # Se não há API key configurada, pedir ao usuário
    if not st.session_state.get("api_key"):
        st.markdown('<div class="cockpit-divider"></div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="aviation-card"><div class="card-title">🔑 API Key (DeepSeek)</div>',
            unsafe_allow_html=True,
        )
        _user_key = st.text_input(
            "Informe sua DeepSeek API Key",
            type="password",
            placeholder="sk-...",
            help="Obtenha em platform.deepseek.com",
        )
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        _user_key = ""

    if st.button("Iniciar Simulado", type="primary", use_container_width=True):
        # Salvar key do usuário se fornecida
        if _user_key:
            st.session_state["api_key"] = _user_key

        if not st.session_state.get("api_key"):
            st.error("Informe a API Key do DeepSeek para continuar.")
        else:
            with st.spinner(f"Carregando manual da {airline}..."):
                chunks = load_chunks(airline)
            if not chunks:
                st.error(f"Nenhum chunk encontrado para {airline}.")
            else:
                st.session_state["chunks"] = chunks
                st.session_state["airline"] = airline
                st.rerun()

    st.markdown("""
    <div class="footer-badge">
        <p class="footer-text">SIMULADO ANAC &bull; PREPARATORIO PARA COMISSARIOS DE VOO</p>
    </div>
    """, unsafe_allow_html=True)

    st.stop()

# ---------------------------------------------------------------------------
# Authenticated — recover saved config
# ---------------------------------------------------------------------------
provider = st.session_state["provider"]
prov_info = PROVIDERS[provider]
api_key = st.session_state["api_key"]
model = st.session_state["model"]

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("""
    <div style="text-align:center;padding:10px 0 20px">
        <div style="font-size:2em">✈️</div>
        <div style="font-family:'Inter',sans-serif;font-weight:800;font-size:1.2em;color:#d4af37;letter-spacing:1px">
            SIMULADO ANAC
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="cockpit-divider"></div>', unsafe_allow_html=True)

    st.markdown(f"""
    <div class="aviation-card" style="padding:14px">
        <div class="card-title" style="font-size:0.9em">⚙️ Motor de IA</div>
        <div style="color:#c8d6e5;font-size:0.9em"><strong>{provider}</strong> — {model}</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="cockpit-divider"></div>', unsafe_allow_html=True)

    with st.expander("🎨 Conteúdo Avançado ➡ Mídia com IA"):
        st.markdown("""
        <div style="color:#8899aa;font-size:0.82em;line-height:1.6;margin-bottom:12px">
            <strong style="color:#d4af37">Imagen</strong> — Gera uma ilustração educacional
            relacionada ao tema da questão (ex: procedimento de evacuação,
            equipamentos de segurança).<br><br>
            <strong style="color:#d4af37">Veo 3</strong> — Gera um vídeo curto (5s)
            mostrando uma cena de aviação civil relacionada ao tema.<br><br>
            Ambos usam a <strong>API do Google AI</strong> e requerem uma
            <strong>GEMINI_API_KEY</strong>. Se o provedor já for Google Gemini,
            a mesma chave é reutilizada. São opcionais — o quiz funciona
            normalmente sem eles.
        </div>
        """, unsafe_allow_html=True)

        enable_image = st.checkbox("Gerar imagem (Imagen)", value=False)
        enable_video = st.checkbox("Gerar video (Veo 3)", value=False)

        # Gemini key for media
        if enable_image or enable_video:
            if provider == "Google Gemini":
                gemini_key = st.session_state["api_key"]
            else:
                gemini_env = os.getenv("GEMINI_API_KEY", "")
                gemini_key = st.text_input(
                    "GEMINI_API_KEY (para midia)",
                    value=gemini_env,
                    type="password",
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

    st.markdown('<div class="cockpit-divider"></div>', unsafe_allow_html=True)

    # Score display
    correct = st.session_state["score_correct"]
    total = st.session_state["score_total"]

    if total > 0:
        pct = correct / total * 100
        pct_color = "#22c55e" if pct >= 70 else "#f59e0b" if pct >= 50 else "#ef4444"
        st.markdown(f"""
        <div class="score-display">
            <div class="score-label">Desempenho</div>
            <div class="score-number" style="color:{pct_color}">{pct:.0f}%</div>
            <div style="color:#8899aa;font-size:0.9em">{correct}/{total} corretas</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="score-display">
            <div class="score-label">Desempenho</div>
            <div style="color:#4a5568;font-size:0.9em;margin-top:8px">Nenhuma questao respondida</div>
        </div>
        """, unsafe_allow_html=True)

    st.write("")
    if st.button("Zerar placar"):
        st.session_state["score_correct"] = 0
        st.session_state["score_total"] = 0
        st.rerun()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _check_api_key():
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
# From here on, chunks are loaded.
# ---------------------------------------------------------------------------
chunks = st.session_state["chunks"]
airline = st.session_state["airline"]

# Header bar
airline_icons = {"GOL": "🟠", "LATAM": "🔴", "Azul": "🔵"}
icon = airline_icons.get(airline, "✈️")

st.markdown(f"""
<div style="display:flex;align-items:center;gap:12px;margin-bottom:20px">
    <span style="font-size:2em">{icon}</span>
    <div>
        <div style="font-family:'Inter',sans-serif;font-size:1.6em;font-weight:800;color:#fff">
            Simulado — {airline}
        </div>
        <div style="font-family:'Inter',sans-serif;font-size:0.85em;color:#d4af37;letter-spacing:2px;text-transform:uppercase">
            Preparatorio ANAC
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown('<div class="cockpit-divider"></div>', unsafe_allow_html=True)

# Topic filter
topics = get_topics(chunks)
topic_options = ["Todos"] + topics
selected_topic = st.selectbox("Filtrar por tema", options=topic_options)
effective_topic = "" if selected_topic == "Todos" else selected_topic
st.session_state["topic_filter"] = effective_topic

# Dynamic background based on topic or current chunk
_bg_topic = effective_topic
if not _bg_topic and st.session_state.get("current_chunk"):
    _bg_topic = st.session_state["current_chunk"].tema or ""
_bg_b64 = _get_bg_for_topic(_bg_topic) if _bg_topic else _load_bg_b64("default.jpg")

if _bg_b64:
    st.markdown(f"""
    <style>
    .stApp {{
        background: linear-gradient(
            rgba(10, 22, 40, 0.88),
            rgba(13, 31, 60, 0.92),
            rgba(10, 22, 40, 0.95)
        ),
        url("data:image/jpeg;base64,{_bg_b64}") !important;
        background-size: cover !important;
        background-position: center !important;
        background-attachment: fixed !important;
    }}
    </style>
    """, unsafe_allow_html=True)

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
        st.warning("Nenhum trecho disponivel para este tema.")
        st.stop()

    with col2:
        generate = st.button("Gerar questao", type="primary")

    if not generate:
        st.markdown("""
        <div class="aviation-card" style="text-align:center;padding:40px">
            <div style="font-size:3em;margin-bottom:12px">📋</div>
            <div style="color:#c8d6e5;font-size:1.1em">Clique em <strong style="color:#d4af37">Gerar questao</strong> para comecar</div>
            <div style="color:#4a5568;font-size:0.85em;margin-top:8px">Questoes geradas por IA com base no manual oficial</div>
        </div>
        """, unsafe_allow_html=True)
        st.stop()

    _check_api_key()
    chunk = pick_random_chunk(filtered)

    with st.spinner("Gerando questao com IA..."):
        try:
            question = generate_question(chunk, provider, api_key, model)
        except Exception as e:
            st.error(f"Erro ao gerar questao: {e}")
            st.stop()

    st.session_state["current_question"] = question
    st.session_state["current_chunk"] = chunk
    st.session_state["answered"] = False
    st.session_state["selected_answer"] = None

    # --- Media generation (optional) ---
    tema = chunk.tema or "aviacao civil"

    if enable_image and gemini_key:
        with st.spinner("Gerando imagem ilustrativa..."):
            try:
                img_bytes = generate_image(
                    question["pergunta"], tema, gemini_key, imagen_model
                )
                st.session_state["current_image"] = img_bytes
            except Exception as e:
                st.warning(f"Imagem nao gerada: {e}")

    if enable_video and gemini_key:
        with st.spinner("Gerando video (pode levar alguns minutos)..."):
            try:
                vid_bytes = generate_video(tema, gemini_key, veo_model)
                st.session_state["current_video"] = vid_bytes
            except Exception as e:
                st.warning(f"Video nao gerado: {e}")

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
                st.image(img, caption="Ilustracao gerada por IA (Imagen)", use_container_width=True)
            idx += 1
        if vid:
            with cols[idx]:
                st.video(vid, format="video/mp4")
                st.caption("Video gerado por IA (Veo 3)")


# ---------------------------------------------------------------------------
# STATE 3 — Show question, await answer
# ---------------------------------------------------------------------------
if not st.session_state["answered"]:
    st.markdown(f"""
    <div class="question-card">
        <div style="color:#d4af37;font-size:0.8em;font-weight:600;letter-spacing:2px;margin-bottom:12px">QUESTAO</div>
        <div class="question-text">{question["pergunta"]}</div>
    </div>
    """, unsafe_allow_html=True)
    _show_media()

    options = [f"{k}) {v}" for k, v in alts.items()]
    choice = st.radio("Escolha uma alternativa:", options, index=None, label_visibility="collapsed")

    if st.button("Confirmar resposta", type="primary"):
        if choice is None:
            st.warning("Selecione uma alternativa antes de confirmar.")
        else:
            letter = choice[0]
            st.session_state["selected_answer"] = letter
            st.session_state["answered"] = True

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

st.markdown(f"""
<div class="question-card">
    <div style="color:#d4af37;font-size:0.8em;font-weight:600;letter-spacing:2px;margin-bottom:12px">RESULTADO</div>
    <div class="question-text">{question["pergunta"]}</div>
</div>
""", unsafe_allow_html=True)
_show_media()

# Show alternatives with aviation-themed color coding
for letter, text in alts.items():
    if letter == correct_letter:
        st.markdown(
            f'<div class="answer-correct"><strong>{letter})</strong> {text} ✅</div>',
            unsafe_allow_html=True,
        )
    elif letter == selected and not is_correct:
        st.markdown(
            f'<div class="answer-wrong"><strong>{letter})</strong> {text} ❌</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div class="answer-neutral"><strong>{letter})</strong> {text}</div>',
            unsafe_allow_html=True,
        )

st.write("")

if is_correct:
    st.success("Parabens! Resposta correta!")
else:
    st.error(f"Resposta correta: **{correct_letter}) {alts[correct_letter]}**")

# Explanation + reference
with st.expander("📖 Explicacao e referencia"):
    st.write(question["explicacao"])
    st.markdown('<div class="cockpit-divider"></div>', unsafe_allow_html=True)
    st.markdown(f"""
    <div style="color:#8899aa;font-size:0.85em">
        <strong style="color:#d4af37">Fonte:</strong> {chunk.fonte}<br>
        <strong style="color:#d4af37">Capitulo:</strong> {chunk.capitulo}<br>
        <strong style="color:#d4af37">Secao:</strong> {chunk.secao_titulo}<br>
        <strong style="color:#d4af37">Paginas:</strong> {chunk.paginas}
    </div>
    """, unsafe_allow_html=True)

if st.button("Proxima questao ➡️", type="primary"):
    _reset_question()
    st.rerun()

st.markdown("""
<div class="footer-badge">
    <p class="footer-text">SIMULADO ANAC &bull; PREPARATORIO PARA COMISSARIOS DE VOO</p>
</div>
""", unsafe_allow_html=True)
