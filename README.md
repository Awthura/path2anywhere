<img src="logo.svg" width="80" alt="Path2Anywhere logo" />

# Path2Anywhere

Google Maps–powered geospatial tools for any industry, any use case.

**Live site → [awthura.github.io/path2anywhere](https://awthura.github.io/path2anywhere)**

---

### Tools

| Tool | Description |
|---|---|
| [Supply Chain Calculator](https://awthura.github.io/path2anywhere/route-calculator/supply-chain/) | Calculate driving distances and durations across a 3-stop route: Supplier → Plant → Customer |
| More coming soon | — |

### Stack

- **Frontend** — Vanilla HTML/CSS/JS, hosted on GitHub Pages
- **Backend** — n8n Cloud webhook (geocoding + distance matrix)
- **APIs** — Google Maps JavaScript API, Places API, Directions API, Distance Matrix API

### Local dev

No build step — open any `index.html` directly in a browser, or serve with:

```bash
npx serve .
```
