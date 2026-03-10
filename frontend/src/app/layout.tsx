import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "אינטרסים לעם",
  description: "מפת ניגודי עניינים של בעלי תפקידים ציבוריים בישראל",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="he" dir="rtl">
      <head>
        <meta name="theme-color" content="#06607C" />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link
          href="https://fonts.googleapis.com/css2?family=Rubik:wght@300;400;500;600;700&display=swap"
          rel="stylesheet"
        />
      </head>
      <body className="bg-[#F6F6F6] text-gray-900 min-h-screen flex flex-col">
        <a
          href="#main-content"
          className="sr-only focus:not-sr-only focus:absolute focus:top-2 focus:right-2 focus:z-[100] focus:bg-primary-700 focus:text-white focus:px-4 focus:py-2 focus:rounded-lg"
        >
          דלג לתוכן
        </a>

        <header className="bg-primary-800 sticky top-0 z-50">
          <nav className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-14 sm:h-16 flex items-center justify-between">
            <a href="/" className="flex items-center gap-2 text-white">
              <svg xmlns="http://www.w3.org/2000/svg" className="w-7 h-7 sm:w-8 sm:h-8" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
              </svg>
              <span className="text-lg sm:text-xl font-bold">אינטרסים לעם</span>
            </a>
            <div className="flex gap-1">
              <a
                href="/"
                className="px-3 sm:px-4 py-2 rounded-lg text-sm font-medium text-primary-100 hover:bg-white/10 hover:text-white transition-colors"
              >
                חיפוש
              </a>
              <a
                href="/graph"
                className="px-3 sm:px-4 py-2 rounded-lg text-sm font-medium text-primary-100 hover:bg-white/10 hover:text-white transition-colors"
              >
                מפת קשרים
              </a>
              <a
                href="/api/docs"
                target="_blank"
                className="px-3 sm:px-4 py-2 rounded-lg text-sm font-medium text-primary-100 hover:bg-white/10 hover:text-white transition-colors"
              >
                API
              </a>
            </div>
          </nav>
        </header>

        <main id="main-content" className="flex-1">
          {children}
        </main>

        <footer className="bg-primary-900 text-primary-100 py-6 text-center text-sm">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            אינטרסים לעם — שקיפות ניגודי עניינים של בעלי תפקידים ציבוריים בישראל
          </div>
        </footer>
      </body>
    </html>
  );
}
