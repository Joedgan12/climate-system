const FontLoader = () => (
  <style>{`
    @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=Source+Sans+3:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --teal-900: #0a3d2e;
      --teal-800: #0e5c44;
      --teal-700: #137a59;
      --teal-600: #1a9970;
      --teal-500: #22b888;
      --teal-400: #4dcba0;
      --teal-100: #e6f7f1;
      --teal-50:  #f0fbf7;
      --white:    #ffffff;
      --gray-50:  #f8f9fa;
      --gray-100: #f0f2f4;
      --gray-200: #dde1e7;
      --gray-400: #8d949e;
      --gray-600: #4a5568;
      --gray-900: #1a2233;
      --red:      #c0392b;
      --orange:   #e67e22;
      --amber:    #f39c12;
      --yellow:   #f1c40f;
      --compatible: #1a9970;
      --almost:   #27ae60;
      --insufficient: #e67e22;
      --highly-insufficient: #e74c3c;
      --critical: #c0392b;
      --font-serif: 'DM Serif Display', Georgia, serif;
      --font-sans:  'Source Sans 3', system-ui, sans-serif;
      --font-mono:  'JetBrains Mono', monospace;
    }

    body {
      font-family: var(--font-sans);
      background: var(--white);
      color: var(--gray-900);
      font-size: 16px;
      line-height: 1.6;
    }

    * { -webkit-font-smoothing: antialiased; }

    a { color: var(--teal-700); text-decoration: none; }
    a:hover { text-decoration: underline; }

    @keyframes fadeUp {
      from { opacity: 0; transform: translateY(24px); }
      to   { opacity: 1; transform: translateY(0); }
    }
    @keyframes fadeIn {
      from { opacity: 0; }
      to   { opacity: 1; }
    }
    @keyframes slideInLeft {
      from { opacity: 0; transform: translateX(-32px); }
      to   { opacity: 1; transform: translateX(0); }
    }
    @keyframes thermometerRise {
      from { height: 0%; }
      to   { height: var(--fill-height); }
    }
    @keyframes counterUp {
      from { opacity: 0; transform: translateY(8px); }
      to   { opacity: 1; transform: translateY(0); }
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50%       { opacity: 0.4; }
    }
    @keyframes scrollTicker {
      from { transform: translateX(100%); }
      to   { transform: translateX(-100%); }
    }
    @keyframes gradientShift {
      0%   { background-position: 0% 50%; }
      50%  { background-position: 100% 50%; }
      100% { background-position: 0% 50%; }
    }
    @keyframes mapPulse {
      0%   { r: 4; opacity: 1; }
      100% { r: 14; opacity: 0; }
    }

    .animate-fadeUp    { animation: fadeUp 0.7s ease forwards; }
    .animate-fadeIn    { animation: fadeIn 0.5s ease forwards; }
    .animate-slideLeft { animation: slideInLeft 0.6s ease forwards; }

    .stagger-1 { animation-delay: 0.1s; opacity: 0; }
    .stagger-2 { animation-delay: 0.2s; opacity: 0; }
    .stagger-3 { animation-delay: 0.3s; opacity: 0; }
    .stagger-4 { animation-delay: 0.4s; opacity: 0; }
    .stagger-5 { animation-delay: 0.5s; opacity: 0; }
    .stagger-6 { animation-delay: 0.6s; opacity: 0; }

    .ticker-wrap { overflow: hidden; white-space: nowrap; }
    .ticker-inner { display: inline-block; animation: scrollTicker 40s linear infinite; }

    section { scroll-margin-top: 72px; }
  `}</style>
);

export default FontLoader;
