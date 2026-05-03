"use client";

import { useEffect, useState } from "react";

const DEFAULT_ABOUT_HTML = `
<h1>אודות הפרויקט "ניגוד עניינים לעם"</h1>

<h2>מהו הפרויקט?</h2>
<p>"ניגוד עניינים לעם" הוא פרויקט שמטרתו לרכז, להציג ולהנגיש לציבור את הסדרי ניגוד העניינים של נבחרי ציבור ועובדי מדינה בכירים בישראל. אנו מאמינים כי חשיפת הצמתים שבהם האינטרס האישי עלול לפגוש את האינטרס הציבורי היא כלי חיוני למניעת שחיתות ולחיזוק אמון הציבור.</p>
<p>הפלטפורמה מרכזת מסמכים המבוססים בעיקרם על בקשות חופש מידע ועל מידע שפורסם באתר "מידע לעם" (<a href="https://www.odata.org.il" target="_blank" rel="noopener noreferrer">odata.org.il</a>), כמו גם הסדרים שפורסמו באופן יזום על ידי המדינה באתר הממשלתי (<a href="https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict" target="_blank" rel="noopener noreferrer">gov.il</a>), מעבדת אותם ומציגה אותם בממשק אחיד, נוח לחיפוש ולניתוח רוחבי.</p>

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
  <li>להציג מפת קשרים ויזואלית ולנווט בה.</li>
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
<p>לדליפות מידע, בעיות נגישות, הארות, הערות, ושלל צרות — <a href="mailto:zomer@octopus.org.il">zomer@octopus.org.il</a>.</p>
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

  const html = content || DEFAULT_ABOUT_HTML;

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
