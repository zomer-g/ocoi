"use client";

import { useEffect, useState } from "react";

// Heuristic: if there's no obvious HTML tag but there are markdown markers,
// treat content as markdown.
function looksLikeMarkdown(s: string): boolean {
  const trimmed = s.trim();
  if (!trimmed) return false;
  const hasHtmlTag = /<\s*(h[1-6]|p|ul|ol|li|div|span|a|strong|em|br)\b/i.test(trimmed);
  if (hasHtmlTag) return false;
  const hasMdMarker =
    /(^|\n)\s*#{1,6}\s/.test(trimmed) || // headings
    /(^|\n)\s*-\s/.test(trimmed) || // list items
    /\*\*[^*]+\*\*/.test(trimmed) || // bold
    /\[[^\]]+\]\([^)]+\)/.test(trimmed); // links
  return hasMdMarker;
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

// Apply inline markdown (bold, links) to a single text run. The input is
// already HTML-escaped.
function renderInline(s: string): string {
  // Links: [text](url) — handles nested-bracket case [[text](u1)](u2) by
  // greedily collapsing to the outermost url.
  s = s.replace(/\[([^\]]*?(?:\[[^\]]*\]\([^)]+\)[^\]]*)?)\]\(([^)]+)\)/g, (_m, text: string, href: string) => {
    // If the inner text itself contains a markdown link, strip it down to
    // its label so we don't render two anchor tags inside one another.
    const cleanText = text.replace(/\[([^\]]+)\]\([^)]+\)/g, "$1");
    // Strip any whitespace (CMS pasters often type "mailto: foo@bar"
    // with a stray space after the colon, which breaks the link).
    const cleanHref = href.replace(/\s+/g, "");
    const isExternal = /^https?:\/\//i.test(cleanHref);
    const attrs = isExternal ? ' target="_blank" rel="noopener noreferrer"' : "";
    return `<a href="${cleanHref}"${attrs}>${cleanText}</a>`;
  });
  // Bold: **text**
  s = s.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  return s;
}

function renderMarkdown(src: string): string {
  const lines = src.replace(/\r\n?/g, "\n").split("\n");
  const out: string[] = [];
  let i = 0;
  let paraBuf: string[] = [];
  let listBuf: string[] = [];

  const flushPara = () => {
    if (paraBuf.length) {
      const text = paraBuf.join(" ").trim();
      if (text) out.push(`<p>${renderInline(escapeHtml(text))}</p>`);
      paraBuf = [];
    }
  };
  const flushList = () => {
    if (listBuf.length) {
      const items = listBuf
        .map((it) => `<li>${renderInline(escapeHtml(it))}</li>`)
        .join("");
      out.push(`<ul>${items}</ul>`);
      listBuf = [];
    }
  };

  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();

    if (!trimmed) {
      flushPara();
      flushList();
      i++;
      continue;
    }

    const heading = /^(#{1,6})\s+(.*)$/.exec(trimmed);
    if (heading) {
      flushPara();
      flushList();
      const level = heading[1].length;
      out.push(`<h${level}>${renderInline(escapeHtml(heading[2]))}</h${level}>`);
      i++;
      continue;
    }

    const listItem = /^[-*]\s+(.*)$/.exec(trimmed);
    if (listItem) {
      flushPara();
      listBuf.push(listItem[1]);
      i++;
      continue;
    }

    flushList();
    paraBuf.push(trimmed);
    i++;
  }
  flushPara();
  flushList();

  return out.join("\n");
}

const DEFAULT_ABOUT_HTML = `
<h1>אודות הפרויקט "ניגוד עניינים לעם"</h1>

<h2>מהו הפרויקט?</h2>
<p>"ניגוד עניינים לעם" הוא פרויקט שמטרתו לרכז, להציג ולהנגיש לציבור את הסדרי ניגוד העניינים של נבחרי ציבור ועובדי מדינה בכירים בישראל. אנו מאמינים כי חשיפת הצמתים שבהם האינטרס האישי עלול לפגוש את האינטרס הציבורי היא כלי חיוני למניעת שחיתות ולחיזוק אמון הציבור.</p>
<p>הפלטפורמה מרכזת מסמכים המבוססים בעיקרם על בקשות חופש מידע ועל מידע שפורסם באתר "מידע לעם" (<a href="https://www.odata.org.il" target="_blank" rel="noopener noreferrer">odata.org.il</a>), כמו גם הסדרים שפורסמו באופן יזום על ידי המדינה באתר הממשלתי (<a href="https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict" target="_blank" rel="noopener noreferrer">gov.il</a>), מעבדת אותם ומציגה אותם בממשק אחיד, נוח לחיפוש ולניתוח רוחבי.</p>

<h2>שכבות הנתונים במערכת</h2>
<p>בכל מפת קשרים מופיעות שתי שכבות נתונים שניתן להציג או להסתיר באמצעות כפתורי סינון בפינת המפה:</p>
<ul>
  <li><strong>הסדרי ניגוד עניינים</strong> — קשרים שחולצו אוטומטית מתוך מסמכי הסדרי ניגוד העניינים של בעלי תפקידים ציבוריים. כל קשר מקושר למסמך מקור בודד שניתן לאמת.</li>
  <li><strong>הוצאות קשר עם הציבור</strong> — קשרים שעולים מתוך דוחות הוצאות חברי הכנסת מתקציב הקשר עם הבוחר (חיוב חברי הכנסת מספקים שונים במימון ציבורי). שכבה זו מתעדת מאיזה ספקים רכש כל חבר/ת כנסת — לצד תקופת הרכישות, סכום מצטבר וקטגוריות.</li>
</ul>
<p>המסננים בראש המפה מאפשרים, למשל, להציג רק את שכבת ההסדרים, רק את שכבת ההוצאות הציבוריות, או את שתיהן יחד כדי לראות חפיפות.</p>

<h2>חשוב לדעת — עיבוד נתונים אוטומטי</h2>
<p>המערכת משתמשת בטכנולוגיות של מודל שפה כדי לחלץ באופן אוטומטי שמות של ישויות, תאגידים וקשרים מתוך אלפי מסמכים סרוקים. חשוב להדגיש כי מדובר בעיבוד ממוחשב, ומשום כך הוא אינו חסין מטעויות, השמטות או זיהויים שגויים. המידע המוצג באתר נועד לשמש ככלי עזר לניווט ולגילוי ראשוני בלבד.</p>
<p>בהתאם לכך, בטבלת הקשרים הצמודה לכל מפה, מופיע קישור ישיר למסמך המקור עליו מבוסס המידע. מסמך המקור הוא המקור היחיד בעל תוקף רשמי ומהימנות מלאה, ואנו ממליצים תמיד לעיין בו לצורך אימות הנתונים.</p>

<h2>למי מיועד האתר?</h2>
<ul>
  <li>אזרחים המעוניינים לוודא שנציגיהם פועלים ללא פניות.</li>
  <li>עיתונאים וגופי תקשורת המחפשים הצלבות מידע לתחקירים על קשרי הון-שלטון.</li>
  <li>חוקרים וארגוני חברה אזרחית המנתחים את טוהר המידות במגזר הציבורי.</li>
  <li>כל מי שמאמין ש"אור השמש הוא המחטא הטוב ביותר".</li>
</ul>

<h2>מה ניתן לעשות כאן?</h2>
<ul>
  <li>לחפש ולעיין בהסדרי ניגוד עניינים לפי שמות אנשים, תאגידים ונושאים.</li>
  <li>לאתר קשרים עסקיים, משפחתיים או אישיים שהוצהרו על ידי בכירים.</li>
  <li>לראות מאיזה ספקים רכשו חברי כנסת מתקציב הקשר עם הבוחר, כולל סכומים וקטגוריות, ולחפש לפי שם של ספק.</li>
  <li>להציג מפת קשרים ויזואלית, ולסנן בה את שכבת ההסדרים, את שכבת ההוצאות הציבוריות, או את שתיהן.</li>
  <li>להציע תיקונים והערות לכל שדה במסמך — ההצעות מועברות לבדיקת מנהלי המאגר.</li>
  <li>לעבוד מול ה-API הציבורי לצרכי תחקיר או מחקר.</li>
</ul>

<h2>מי עומד מאחורי הפרויקט?</h2>
<p>את הפרויקט מוביל <strong>עו"ד גיא זומר</strong>, מייסד עמותת התמנון, עמותת הצלחה והתנועה לחופש המידע. הפרויקט הוא חלק ממשפחת מיזמים אקטיביסטיים בממשק שבין דאטה, משפט וטכנולוגיה — לעיון בכלל המיזמים: <a href="https://www.z-g.co.il/projects" target="_blank" rel="noopener noreferrer">https://www.z-g.co.il/projects</a>.</p>

<h2>משפחת המיזמים "לעם"</h2>
<p>"ניגוד עניינים לעם" הוא חלק ממשפחת מיזמים שמטרתם הנגשת מידע ציבורי ושקיפות שלטונית בישראל:</p>
<ul>
  <li><strong><a href="https://www.odata.org.il" target="_blank" rel="noopener noreferrer">מידע לעם</a></strong> — פלטפורמת המידע הפתוח שמהווה תשתית הנתונים של המשפחה כולה. שואב ומאחסן מסמכי חופש מידע ומאגרים ממשלתיים.</li>
  <li><strong><a href="https://www.over.org.il" target="_blank" rel="noopener noreferrer">גרסאות לעם</a></strong> — מעקב אחרי שינויים שקטים במאגרי <a href="http://data.gov.il" target="_blank" rel="noopener noreferrer">data.gov.il</a>, כדי שהשקיפות הממשלתית תהיה אחריותית גם לאורך זמן.</li>
  <li><strong><a href="https://ocal.org.il" target="_blank" rel="noopener noreferrer">יומן לעם</a></strong> — לוח שנה מאוחד של יומני הפגישות של נבחרי ציבור ועובדי מדינה בכירים, להשלמת התמונה לצד הסדרי ניגוד העניינים.</li>
</ul>

<h2>מדיניות פרטיות</h2>
<p>השימוש באתר עשוי לכלול איסוף מידע אודות משתמשים, וכן שימוש ועיבוד של חומרים ומסמכים שהועלו למערכת לצורך הפעלת השירות ושיפורו.</p>
<ul>
  <li><strong>מידע שנאסף</strong>: מידע טכני על השימוש באתר וכן מסמכים ותוכן שהמשתמש מעלה למערכת.</li>
  <li><strong>שימוש במידע</strong>: להפעלת השירות, שיפורו, ניתוח פעילות המערכת ועיבוד מסמכים לצורך חיפוש וסיווג.</li>
  <li><strong>אבטחת מידע</strong>: ננקטים אמצעים סבירים להגנה על המידע, אולם לא ניתן להבטיח הגנה מוחלטת מפני אירועי אבטחה.</li>
</ul>

<h2>קוד מקור</h2>
<p><a href="https://github.com/zomer-g/ocoi" target="_blank" rel="noopener noreferrer">https://github.com/zomer-g/ocoi</a>.</p>

<h2>נגישות</h2>
<p>אתר זה נבנה בהתאם להנחיות נגישות WCAG 2.1 ברמה AA. אם נתקלתם בבעיית נגישות, אנא פנו אלינו כדי שנוכל לתקן ולשפר.</p>

<h2>יצירת קשר</h2>
<p>לדליפות מידע, בעיות נגישות, הארות, הערות, ושלל צרות — <a href="mailto:guy@z-g.co.il">guy@z-g.co.il</a>.</p>
`;

export default function AboutPage() {
  const [content, setContent] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/v1/site/content/about_content")
      .then((r) => r.json())
      .then((d) => {
        setContent(d?.data?.value || "");
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const raw = content || DEFAULT_ABOUT_HTML;
  const html = looksLikeMarkdown(raw) ? renderMarkdown(raw) : raw;

  return (
    <>
      <section className="bg-gradient-to-b from-primary-800 to-primary-700 py-10 px-4">
        <div className="max-w-4xl mx-auto text-center">
          <h1 className="text-2xl sm:text-3xl font-bold text-white mb-2">אודות</h1>
          <p className="text-primary-200 text-sm sm:text-base">
            אודות הפרויקט ניגוד עניינים לעם
          </p>
        </div>
      </section>

      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {loading ? (
          <div className="text-center py-12 text-gray-400">טוען...</div>
        ) : (
          <div
            className="prose prose-lg max-w-none bg-white rounded-xl shadow-sm border border-gray-200 p-6 sm:p-8"
            dir="rtl"
            dangerouslySetInnerHTML={{ __html: html }}
          />
        )}
      </div>
    </>
  );
}
