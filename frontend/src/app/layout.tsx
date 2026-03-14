import type { Metadata } from "next";
import Script from "next/script";
import SiteShell from "@/components/SiteShell";
import "./globals.css";

const GA_ID = "G-PLRGJXQMGB";

export const metadata: Metadata = {
  title: "ניגוד עניינים לעם",
  description: "מפת ניגודי עניינים של בעלי תפקידים ציבוריים בישראל",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="he" dir="rtl">
      <head>
        <link rel="icon" href="/favicon.svg" type="image/svg+xml" />
        <meta name="theme-color" content="#044E66" />
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link
          href="https://fonts.googleapis.com/css2?family=Rubik:wght@300;400;500;600;700&display=swap"
          rel="stylesheet"
        />
      </head>
      <body className="bg-[#F6F6F6] text-gray-900 min-h-screen flex flex-col">
        <Script src={`https://www.googletagmanager.com/gtag/js?id=${GA_ID}`} strategy="afterInteractive" />
        <Script id="gtag-init" strategy="afterInteractive">
          {`window.dataLayer=window.dataLayer||[];function gtag(){dataLayer.push(arguments);}gtag('js',new Date());gtag('config','${GA_ID}');`}
        </Script>
        <a
          href="#main-content"
          className="sr-only focus:not-sr-only focus:absolute focus:top-2 focus:right-2 focus:z-[100] focus:bg-primary-700 focus:text-white focus:px-4 focus:py-2 focus:rounded-lg"
        >
          דלג לתוכן
        </a>

        <SiteShell>{children}</SiteShell>
      </body>
    </html>
  );
}
