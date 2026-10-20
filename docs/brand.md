# Grontia brand

The official brand kit lives in [`web/static/brand/`](../web/static/brand/), where the web app serves it.

## Colours

| Token | Hex | Use |
|---|---|---|
| `--grontia-navy` (`--nl-blue` in the app) | `#003DA5` | Brand, headings, primary buttons, neutral states |
| `--grontia-red` (`--nl-red` in the app) | `#D71920` | Gable roof, "above the limit" verdicts, the address marker |
| `--grontia-white` | `#FFFFFF` | Backgrounds, the white stripe of the flag bar |

Orange (`--nl-orange`, `#E8731A`) is the national accent for "watch out" states and hints. Both brand colours pass WCAG AA for text on white (red 5.2:1, navy 9.5:1); orange (3.0:1) is only used for borders and accents, never for text. The dark theme uses lighter variants defined in `web/static/style.css`.

## Files

| File | Where it is used |
|---|---|
| `grontia-symbol.svg` | Header of the web app |
| `grontia-wordmark.svg` | PDF report header, top of the README |
| `grontia-wordmark.png` | Link previews (`og:image`) |
| `favicon.svg`, `favicon-16/32/48.png` | Browser tab (SVG first, PNG fallbacks) |
| `favicon-180.png` | Apple touch icon |
| `favicon-192/512.png` | Web app manifest (`web/static/site.webmanifest`) |
| `grontia-symbol-256/512/1024.png` | For use outside the site (slides, social profiles) |
| `brand-colors.css` | Source of the colour tokens |

## Usage

- Keep the symbol on white or on the navy; don't recolour it.
- The name is set in bold capitals with wide tracking, as in the wordmark.
- Grontia uses the colours of the Dutch flag but is not a government service: don't combine it with the Rijkshuisstijl or the Rijksoverheid logo.
