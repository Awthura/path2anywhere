<img src="logo.svg" width="80" alt="Path2Anywhere logo" />

# Path2Anywhere

Google Maps–powered geospatial tools for any industry, any use case.

**Live site → [awthura.github.io/path2anywhere](https://awthura.github.io/path2anywhere)**

---

### Tools

#### Route Calculator
| Tool | Description |
|---|---|
| [Supply Chain Calculator](https://awthura.github.io/path2anywhere/route-calculator/supply-chain/) | 3-stop route (Supplier → Plant → Customer) with distance, duration, fuel consumption, and CO₂ footprint |
| [Commute Calculator](https://awthura.github.io/path2anywhere/route-calculator/commute/) | Round-trip commute (Home ↔ Work) with daily, weekly, and monthly fuel and CO₂ estimates |

### Stack

- **Frontend** — Vanilla HTML/CSS/JS, hosted on GitHub Pages
- **Backend** — n8n Cloud webhook (geocoding + distance matrix)
- **APIs** — Google Maps JavaScript API, Places API, Directions API, Distance Matrix API
