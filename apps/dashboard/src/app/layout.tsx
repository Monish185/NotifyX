import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'NotifyX — Notification Infrastructure Platform',
  description: 'Developer-facing notification infrastructure platform for Email, Push, SMS, and In-App delivery.',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen bg-slate-950 text-slate-100 antialiased font-sans">
        {children}
      </body>
    </html>
  );
}
