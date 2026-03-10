# OCOI Design System

Shared design language with [OCAL](https://ocal.org.il/) — both are part of the "לעם" transparency platform family.

## Colors

### Primary Palette (Teal/Cyan)

| Token         | Hex       | Usage                                      |
|---------------|-----------|---------------------------------------------|
| primary-50    | #E8F5F9   | Light tints, hover backgrounds              |
| primary-100   | #CCEBF3   | Active chip bg, badge bg, footer text       |
| primary-200   | #9CD9EA   | Borders, secondary text on dark bg          |
| primary-300   | #68C4DE   | Focus rings                                 |
| primary-400   | #38AED0   | Light accent                                |
| primary-500   | #1094B8   | Checkboxes, focus rings                     |
| primary-600   | #0A7A9A   | Links, subtitle text                        |
| primary-700   | #06607C   | **Key brand color** — buttons, badges, hero gradient end |
| primary-800   | #044E66   | Header bg, hero gradient start              |
| primary-900   | #003647   | Footer bg                                   |

### Neutrals

| Token    | Hex       | Usage                        |
|----------|-----------|-------------------------------|
| gray-50  | #F9FAFB   | Badge bg                     |
| gray-100 | #F3F4F6   | Dividers                     |
| gray-200 | #E5E7EB   | Borders                      |
| gray-300 | #D1D5DB   | Input borders                |
| gray-400 | #9CA3AF   | Placeholder text, icons      |
| gray-500 | #6B7280   | Muted text                   |
| gray-600 | #4B5563   | Secondary text               |
| gray-700 | #374151   | Body text                    |
| gray-900 | #111827   | Headings                     |
| page-bg  | #F6F6F6   | Page background              |

### Entity Type Colors

| Type        | Color   | Hex     |
|-------------|---------|---------|
| person      | Blue    | #3B82F6 |
| company     | Green   | #10B981 |
| association | Purple  | #8B5CF6 |
| domain      | Amber   | #F59E0B |

## Typography

**Primary font:** Rubik (Google Fonts)
**Fallback stack:** Rubik, Heebo, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif
**Weights loaded:** 300, 400, 500, 600, 700

### Scale

| Element          | Tailwind                                  | Weight    |
|------------------|-------------------------------------------|-----------|
| Hero title       | text-2xl sm:text-3xl lg:text-4xl          | bold      |
| Page heading     | text-3xl                                  | bold      |
| Section heading  | text-xl                                   | semibold  |
| Card title       | text-base                                 | medium    |
| Body             | text-base (16px)                          | regular   |
| Nav links        | text-sm                                   | medium    |
| Metadata         | text-sm                                   | regular   |
| Labels           | text-xs                                   | regular   |
| Tiny badges      | text-[10px]                               | —         |

## Layout

- **Direction:** RTL (`dir="rtl"`, `lang="he"`)
- **Font:** Rubik (optimized for Hebrew)
- **Page bg:** #F6F6F6
- **Max width:** max-w-7xl (1280px) with mx-auto and responsive padding (px-4 sm:px-6 lg:px-8)
- **Header:** Sticky, bg-primary-800, white text
- **Footer:** bg-primary-900, text-primary-100
- **Hero section:** Gradient from primary-800 to primary-700, white text, centered

## Components

### Cards
```
bg-white rounded-lg border border-gray-200 hover:shadow-sm transition-shadow p-4
```

### Buttons — Primary
```
px-6 py-3 bg-primary-700 text-white rounded-lg hover:bg-primary-800 transition-colors
```

### Inputs
```
w-full px-4 py-3 border border-gray-300 rounded-lg text-lg
focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent
```

### Badges/Pills
```
text-xs px-2 py-0.5 rounded-full border
```

### Nav links
```
px-4 py-2 rounded-lg text-sm font-medium transition-colors
Active: bg-white/15 text-white
Inactive: text-primary-100 hover:bg-white/10 hover:text-white
```

## Shadows & Borders

| Pattern   | Usage                  |
|-----------|------------------------|
| shadow-sm | Default card shadow    |
| shadow-md | Hover card shadow      |
| shadow-lg | Hero search, modals    |
| rounded-md | Small inputs          |
| rounded-lg | Cards, buttons, inputs |
| rounded-xl | Hero input, modals     |
| rounded-full | Badges, dots        |

## Accessibility

- Skip-to-content link
- Focus-visible outlines: 3px solid #06607C with 2px offset
- Reduced motion: prefers-reduced-motion disables animations
- Semantic HTML: nav, main, footer
- ARIA attributes on interactive elements
