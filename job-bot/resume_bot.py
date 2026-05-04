"""
ATS-Friendly Resume Generator Telegram Bot
==========================================
Requirements:
    pip install python-telegram-bot==20.7 fpdf2==2.7.9 python-dotenv

Environment Variables:
    BOT_TOKEN   - Telegram bot token from @BotFather
    WEBHOOK_URL - Your Render public URL, e.g. https://your-app.onrender.com
"""

import os
import logging
import tempfile
from typing import Optional
from fpdf import FPDF
from dotenv import load_dotenv

from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

load_dotenv()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Conversation States
# ──────────────────────────────────────────────
(
    FULL_NAME,
    EMAIL,
    PHONE,
    LOCATION,
    LINKEDIN,
    SUMMARY,
    SKILLS,
    EXP_COUNT,
    EXP_TITLE,
    EXP_COMPANY,
    EXP_DATES,
    EXP_BULLETS,
    EDU_DEGREE,
    EDU_SCHOOL,
    EDU_YEAR,
    CERTS,
    CONFIRM,
) = range(17)


# ──────────────────────────────────────────────
# PDF Generator (fpdf2 - plain text, ATS-safe)
# ──────────────────────────────────────────────
class ATSResumePDF(FPDF):
    """
    Generates a simple, single-column PDF resume.
    - No images, tables, headers/footers with graphics.
    - Standard Helvetica font — guaranteed parseable by ATS parsers
      (Greenhouse, Lever, Workday, iCIMS, etc.)
    """

    MARGIN = 15
    PAGE_WIDTH = 210  # A4 mm

    def __init__(self):
        super().__init__(unit="mm", format="A4")
        self.set_margins(self.MARGIN, self.MARGIN, self.MARGIN)
        self.set_auto_page_break(auto=True, margin=self.MARGIN)
        self.add_page()

    # ── Helpers ──────────────────────────────
    def section_title(self, title: str):
        self.ln(3)
        self.set_font("Helvetica", "B", 11)
        self.cell(0, 6, title.upper(), ln=True)
        # Thin horizontal rule
        self.set_draw_color(80, 80, 80)
        self.set_line_width(0.3)
        self.line(self.MARGIN, self.get_y(), self.PAGE_WIDTH - self.MARGIN, self.get_y())
        self.ln(2)

    def body_line(self, text: str, bold: bool = False, size: int = 10):
        self.set_font("Helvetica", "B" if bold else "", size)
        self.multi_cell(0, 5, text)

    def bullet(self, text: str):
        self.set_font("Helvetica", "", 10)
        self.set_x(self.MARGIN + 4)
        self.multi_cell(0, 5, f"- {text}")

    # ── Sections ─────────────────────────────
    def header_block(self, data: dict):
        self.set_font("Helvetica", "B", 16)
        self.cell(0, 8, data["full_name"], ln=True, align="C")
        contact_parts = [data["email"], data["phone"]]
        if data.get("location"):
            contact_parts.append(data["location"])
        if data.get("linkedin"):
            contact_parts.append(data["linkedin"])
        self.set_font("Helvetica", "", 9)
        self.cell(0, 5, "  |  ".join(contact_parts), ln=True, align="C")
        self.ln(2)

    def summary_section(self, text: str):
        self.section_title("Professional Summary")
        self.body_line(text)

    def skills_section(self, skills: str):
        self.section_title("Core Competencies & Skills")
        self.body_line(skills)

    def experience_section(self, experiences: list):
        self.section_title("Professional Experience")
        for exp in experiences:
            self.body_line(f"{exp['title']} — {exp['company']}", bold=True)
            self.set_font("Helvetica", "I", 9)
            self.cell(0, 5, exp["dates"], ln=True)
            for b in exp.get("bullets", []):
                if b.strip():
                    self.bullet(b.strip())
            self.ln(2)

    def education_section(self, edu: dict):
        self.section_title("Education")
        self.body_line(f"{edu['degree']} — {edu['school']}", bold=True)
        self.set_font("Helvetica", "", 10)
        self.cell(0, 5, edu["year"], ln=True)

    def certifications_section(self, certs: str):
        if certs and certs.lower() not in ("no", "none", "لا", "لا يوجد"):
            self.section_title("Certifications & Licenses")
            for cert in certs.split(","):
                if cert.strip():
                    self.bullet(cert.strip())


def build_pdf(data: dict) -> bytes:
    """Build the PDF and return raw bytes."""
    pdf = ATSResumePDF()
    pdf.header_block(data)
    if data.get("summary"):
        pdf.summary_section(data["summary"])
    if data.get("skills"):
        pdf.skills_section(data["skills"])
    if data.get("experiences"):
        pdf.experience_section(data["experiences"])
    if data.get("education"):
        pdf.education_section(data["education"])
    if data.get("certs"):
        pdf.certifications_section(data["certs"])
    return bytes(pdf.output())


# ──────────────────────────────────────────────
# Conversation Handlers
# ──────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text(
        "👋 مرحباً! سأساعدك في إنشاء سيرة ذاتية ATS-Friendly بصيغة PDF.\n\n"
        "أرسل /cancel في أي وقت للإلغاء.\n\n"
        "📝 *ما اسمك الكامل؟*",
        parse_mode="Markdown",
    )
    return FULL_NAME


async def get_full_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["full_name"] = update.message.text.strip()
    await update.message.reply_text("📧 *ما هو بريدك الإلكتروني؟*", parse_mode="Markdown")
    return EMAIL


async def get_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["email"] = update.message.text.strip()
    await update.message.reply_text("📱 *ما هو رقم هاتفك؟*", parse_mode="Markdown")
    return PHONE


async def get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["phone"] = update.message.text.strip()
    await update.message.reply_text(
        "📍 *ما هو موقعك (المدينة، الدولة)؟*\n_(اختياري — أرسل - للتخطي)_",
        parse_mode="Markdown",
    )
    return LOCATION


async def get_location(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    val = update.message.text.strip()
    context.user_data["location"] = "" if val == "-" else val
    await update.message.reply_text(
        "🔗 *رابط LinkedIn أو الموقع الشخصي؟*\n_(اختياري — أرسل - للتخطي)_",
        parse_mode="Markdown",
    )
    return LINKEDIN


async def get_linkedin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    val = update.message.text.strip()
    context.user_data["linkedin"] = "" if val == "-" else val
    await update.message.reply_text(
        "✍️ *اكتب ملخصاً مهنياً موجزاً (3-4 جمل):*\n"
        "_مثال: مهندس برمجيات بخبرة 5 سنوات في تطوير تطبيقات الويب..._",
        parse_mode="Markdown",
    )
    return SUMMARY


async def get_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["summary"] = update.message.text.strip()
    await update.message.reply_text(
        "🛠 *اذكر مهاراتك الأساسية مفصولة بفاصلة:*\n"
        "_مثال: Python, SQL, React, إدارة المشاريع, Agile_",
        parse_mode="Markdown",
    )
    return SKILLS


async def get_skills(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["skills"] = update.message.text.strip()
    await update.message.reply_text(
        "💼 *كم عدد الخبرات العملية التي تريد إضافتها؟* (1-5)",
        parse_mode="Markdown",
    )
    return EXP_COUNT


async def get_exp_count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        count = int(update.message.text.strip())
        count = max(1, min(count, 5))
    except ValueError:
        await update.message.reply_text("أرجو إدخال رقم بين 1 و5.")
        return EXP_COUNT
    context.user_data["exp_total"] = count
    context.user_data["exp_current"] = 0
    context.user_data["experiences"] = []
    await update.message.reply_text(
        f"*الخبرة 1 من {count}*\n📌 ما هو المسمى الوظيفي؟",
        parse_mode="Markdown",
    )
    return EXP_TITLE


async def get_exp_title(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["_cur_exp"] = {"title": update.message.text.strip()}
    await update.message.reply_text("🏢 اسم الشركة أو المؤسسة؟")
    return EXP_COMPANY


async def get_exp_company(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["_cur_exp"]["company"] = update.message.text.strip()
    await update.message.reply_text(
        "📅 فترة العمل؟\n_مثال: يناير 2021 – مارس 2023_",
        parse_mode="Markdown",
    )
    return EXP_DATES


async def get_exp_dates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["_cur_exp"]["dates"] = update.message.text.strip()
    await update.message.reply_text(
        "📋 اذكر 2-4 إنجازات أو مسؤوليات (كل واحدة في سطر منفصل):\n"
        "_مثال:_\n"
        "_- طورت API خفّض وقت الاستجابة بنسبة 40%_\n"
        "_- قدت فريقاً من 5 مطورين_",
        parse_mode="Markdown",
    )
    return EXP_BULLETS


async def get_exp_bullets(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    bullets = [
        line.lstrip("-• ").strip()
        for line in update.message.text.split("\n")
        if line.strip()
    ]
    context.user_data["_cur_exp"]["bullets"] = bullets
    context.user_data["experiences"].append(context.user_data.pop("_cur_exp"))

    current = len(context.user_data["experiences"])
    total = context.user_data["exp_total"]

    if current < total:
        await update.message.reply_text(
            f"*الخبرة {current + 1} من {total}*\n📌 ما هو المسمى الوظيفي؟",
            parse_mode="Markdown",
        )
        return EXP_TITLE
    else:
        await update.message.reply_text(
            "🎓 *التعليم*\nاكتب درجتك العلمية والتخصص:\n_مثال: بكالوريوس علوم الحاسب_",
            parse_mode="Markdown",
        )
        return EDU_DEGREE


async def get_edu_degree(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["edu_degree"] = update.message.text.strip()
    await update.message.reply_text("🏫 اسم الجامعة أو المعهد؟")
    return EDU_SCHOOL


async def get_edu_school(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["edu_school"] = update.message.text.strip()
    await update.message.reply_text("📅 سنة التخرج؟\n_مثال: 2019 أو 2020-2024_", parse_mode="Markdown")
    return EDU_YEAR


async def get_edu_year(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["edu_year"] = update.message.text.strip()
    await update.message.reply_text(
        "🏅 هل لديك شهادات احترافية؟ اذكرها مفصولة بفاصلة:\n"
        "_مثال: PMP, AWS Certified Developer, أو أرسل 'لا يوجد'_",
        parse_mode="Markdown",
    )
    return CERTS


async def get_certs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["certs"] = update.message.text.strip()
    # Build summary for confirmation
    d = context.user_data
    summary_text = (
        f"✅ *مراجعة السيرة الذاتية:*\n\n"
        f"👤 *الاسم:* {d['full_name']}\n"
        f"📧 *البريد:* {d['email']}\n"
        f"📱 *الهاتف:* {d['phone']}\n"
        f"💼 *الخبرات:* {len(d['experiences'])} خبرة\n"
        f"🎓 *التعليم:* {d['edu_degree']} — {d['edu_school']}\n\n"
        f"هل تريد توليد السيرة الذاتية؟"
    )
    keyboard = [["✅ نعم، أنشئ PDF", "❌ إلغاء"]]
    await update.message.reply_text(
        summary_text,
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True),
    )
    return CONFIRM


async def confirm_and_generate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message.text.startswith("❌"):
        await update.message.reply_text(
            "تم الإلغاء. أرسل /start للبدء من جديد.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return ConversationHandler.END

    await update.message.reply_text("⏳ جاري إنشاء السيرة الذاتية...", reply_markup=ReplyKeyboardRemove())

    d = context.user_data
    data = {
        "full_name": d["full_name"],
        "email": d["email"],
        "phone": d["phone"],
        "location": d.get("location", ""),
        "linkedin": d.get("linkedin", ""),
        "summary": d.get("summary", ""),
        "skills": d.get("skills", ""),
        "experiences": d.get("experiences", []),
        "education": {
            "degree": d["edu_degree"],
            "school": d["edu_school"],
            "year": d["edu_year"],
        },
        "certs": d.get("certs", ""),
    }

    try:
        pdf_bytes = build_pdf(data)
        safe_name = "".join(c for c in d["full_name"] if c.isalnum() or c in " _-").replace(" ", "_")
        filename = f"{safe_name}_Resume.pdf"

        await update.message.reply_document(
            document=pdf_bytes,
            filename=filename,
            caption=(
                "✅ *سيرتك الذاتية جاهزة!*\n\n"
                "💡 *نصائح ATS:*\n"
                "• قدّم الملف بصيغة PDF كما هو\n"
                "• لا تضغه أو تحوله لصورة\n"
                "• تأكد أن المسمى الوظيفي يطابق إعلان الوظيفة\n"
                "• أرسل /start لإنشاء سيرة ذاتية جديدة"
            ),
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error("PDF generation failed: %s", e, exc_info=True)
        await update.message.reply_text(
            "❌ حدث خطأ أثناء إنشاء الملف. أرسل /start وحاول مجدداً."
        )

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text(
        "تم الإلغاء. أرسل /start للبدء من جديد.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return ConversationHandler.END


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled exception:", exc_info=context.error)


# ──────────────────────────────────────────────
# Application Entry Point
# ──────────────────────────────────────────────

def main():
    token = os.environ["BOT_TOKEN"]
    webhook_url = os.environ.get("WEBHOOK_URL", "").rstrip("/")
    port = int(os.environ.get("PORT", 8443))

    app = Application.builder().token(token).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            FULL_NAME:   [MessageHandler(filters.TEXT & ~filters.COMMAND, get_full_name)],
            EMAIL:       [MessageHandler(filters.TEXT & ~filters.COMMAND, get_email)],
            PHONE:       [MessageHandler(filters.TEXT & ~filters.COMMAND, get_phone)],
            LOCATION:    [MessageHandler(filters.TEXT & ~filters.COMMAND, get_location)],
            LINKEDIN:    [MessageHandler(filters.TEXT & ~filters.COMMAND, get_linkedin)],
            SUMMARY:     [MessageHandler(filters.TEXT & ~filters.COMMAND, get_summary)],
            SKILLS:      [MessageHandler(filters.TEXT & ~filters.COMMAND, get_skills)],
            EXP_COUNT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, get_exp_count)],
            EXP_TITLE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, get_exp_title)],
            EXP_COMPANY: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_exp_company)],
            EXP_DATES:   [MessageHandler(filters.TEXT & ~filters.COMMAND, get_exp_dates)],
            EXP_BULLETS: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_exp_bullets)],
            EDU_DEGREE:  [MessageHandler(filters.TEXT & ~filters.COMMAND, get_edu_degree)],
            EDU_SCHOOL:  [MessageHandler(filters.TEXT & ~filters.COMMAND, get_edu_school)],
            EDU_YEAR:    [MessageHandler(filters.TEXT & ~filters.COMMAND, get_edu_year)],
            CERTS:       [MessageHandler(filters.TEXT & ~filters.COMMAND, get_certs)],
            CONFIRM:     [MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_and_generate)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    app.add_handler(conv_handler)
    app.add_error_handler(error_handler)

    if webhook_url:
        # ── Webhook mode (Render / production) ──────────────────────────
        logger.info("Starting in WEBHOOK mode on port %s", port)
        app.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=token,                        # secret path
            webhook_url=f"{webhook_url}/{token}",  # Telegram calls this URL
        )
    else:
        # ── Polling mode (local development) ────────────────────────────
        logger.info("Starting in POLLING mode (local dev)")
        app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
