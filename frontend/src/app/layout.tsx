import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "אינטרסים לעם",
  description: "מפת ניגודי עניינים של בעלי תפקידים ציבוריים בישראל",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="he" dir="rtl">
      <body className="bg-gray-50 text-gray-900 min-h-screen">
        <header className="bg-white border-b border-gray-200 sticky top-0 z-50">
          <nav className="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between">
            <a href="/" className="text-xl font-bold text-blue-700">
              אינטרסים לעם
            </a>
            <div className="flex gap-4 text-sm">
              <a href="/" className="hover:text-blue-600">חיפוש</a>
              <a href="/graph" className="hover:text-blue-600">מפת קשרים</a>
              <a href="/api/docs" className="hover:text-blue-600" target="_blank">API</a>
            </div>
          </nav>
        </header>
        <main>{children}</main>
      </body>
    </html>
  );
}
