# Website Maintenance Guide

This guide provides step-by-step instructions for common maintenance tasks to ensure your site remains stable and functional.

## 🚀 General Workflow

Always follow this process to avoid breaking production:

1. **Start Local Server**: Run `blogdown::serve_site()` in RStudio/Positron.
2. **Make Changes**: Edit files as described below.
3. **Verify Locally**: Check `localhost` to ensure changes look correct.
4. **Commit & Push**: Use Git to push changes to GitHub.
5. **Deploy**: Netlify will automatically build and deploy your changes.

---

## 1. Adding a New Solution
>
> [!IMPORTANT]
> Solutions require updates in **TWO** places: the content file and the data file. If you miss one, the filtering or the link will break.

### Step 1: Create the Content Page

Create a new markdown file in `content/solutions/`.
**Example:** `content/solutions/predictive-maintenance.md`

```yaml
---
title: Predictive Maintenance
image: /images/illustrations/mockups/predictive-maintenance.png
include_cta: true
type: page
layout: single
---

# Your Content Here
Describe the challenge, solution, and ROI...
```

### Step 2: Register in Portfolio Data (Crucial for Grid & Filtering)

Open `data/portfolio.yaml` and add a new entry under `projects:`.
**Ensure the categories and domains match exactly what is defined at the top of the file.**

```yaml
  - title: Predictive Maintenance
    categories:
      - Time Series Forecasting
    domains:
      - Manufacturing
      - Transport & Logistics
    image: /images/illustrations/mockups/predictive-maintenance.png
    summary: AI system that predicts equipment failure before it happens.
    page: /solutions/predictive-maintenance
```

---

## 2. Adding a New Blog Article

Search functionality and the main blog page automatically index new blog posts.

1. Create a new file in `content/blog/`.
2. Use the following front matter:

```yaml
---
title: "My New Article Title"
date: 2024-03-20
section: blog
type: blog
image: img/illustrations/my-image.jpg  # Used for the card thumbnail
draft: false 
---

# Article Content
Your article content goes here...
```

> [!NOTE]
> **Hover Effect**: Valid blog posts (type: blog) will automatically have the clickable "tile" behavior with hover animations on the main blog page.

---

## 2a. Configuring the Main Blog Page

The main blog page (`/blog`) has several configurable elements.

### Hero Image & Call to Action (CTA)

These are controlled by the file: `content/blog/_index.md`.

```yaml
---
title: "The AI Business Scientist..." # The main title text
image: /images/illustrations/my-hero.jpg # Background image for hero section
include_cta: true # Set to false to hide the "Ready to talk?" section
---
```

### "Search blog posts..." Placeholder Text

To change the text inside the search bar, you must edit the layout file directly.
**File:** `themes/hugo-fresh-data-science/layouts/blog/list.html`
**Action:** Search for `<input ... placeholder="Search blog posts...">` (around line 45) and edit the text inside the quotes.

---

## 3. Updating Services

Services are managed in a single file: `content/consulting.md`.

1. Open `content/consulting.md`.
2. Locate the `services:` list in the front matter (top of the file).
3. Add or edit an item:

```yaml
  - title: New Service Name
    icon: rocket  # FontAwesome icon name (e.g., chart-line, cogs)
    subtitle: Brief subtitle
    description: Full description of the service.
    outcome: What the client gets.
    link: "#anchor-link"
```

---

## 4. Adding a New Standard Page (e.g., Terms)

1. Create a new file in `content/` (e.g., `content/terms.md`).
2. Add standard front matter:

```yaml
---
title: Terms and Conditions
type: page
layout: single
---

# Terms and Conditions
...content...
```

---

## 5. Updating Privacy Policy

1. Open `content/privacy.md`.
2. Edit the markdown content below the front matter.
3. **Do not change** `type: testimonials` if that is what controls the layout, unless you are restyling the page.

---

## 6. Updating Global Elements (Footer/Copyright)

### Copyright Text

The footer copyright text is located in a theme partial.
**File:** `themes/hugo-fresh-data-science/layouts/partials/footer.html`
**Action:** Search for "Copyright" (around line 18). You can change the text there. Note that `{{ now.Format "2006" }}` automatically generates the current year.

---

## 🛑 Troubleshooting & Safety

- **Filtering Issues**: If the Solutions grid stops filtering correctly, check `data/portfolio.yaml` for typos in `categories` or `domains`. They must match the list at the top of that file exactly.
- **Breaking Changes**: Avoid editing `themes/` files unless necessary. If you must, duplicate the file as a backup first.
- **Build Errors**: If `serve_site()` fails, check the R console output. Common errors include invalid YAML indentation.
