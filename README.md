# Doom Scrolling: My Digital Wellbeing Analysis
![doomscrolling](https://github.com/user-attachments/assets/c91ccce3-7b5a-405d-9fad-ed89d8465c59)

## Screen Time Data Science Project (@_@)

**Looking at My Own Screen Time: September vs October 2025**

---

## Why This Project (⊙_⊙)

I wanted to see how I actually spend time on my phone. Like many students and young adults, I doomscroll, watch, and tap just enough to make me a high-functioning zombie. The problem? I had no clue how much time was actually slipping away.

This project looks at my personal Samsung screen time over two months to figure out:
* Which apps I use the most
* When I waste the most time
* How my habits affect my productivity and wellbeing

### The Problem I'm Trying to Solve ㄟ( ▔, ▔ )ㄏ

I knew I was spending a lot of time on my phone, but I didn't really know which apps were eating my time or when I'm most distracted.

### How I Approached It ？( ▔ . ▔ )？ (Data Collection)

* Collected my own screen time data from an app called [Stayfree](https://stayfreeapps.com/) (Android) that tracks your phone and app usage on a month-by-month basis and lets you download it as a clean CSV file
<img width="200" height="500" alt="image" src="https://github.com/user-attachments/assets/38574bdb-8476-4934-b411-f5077dfaf597" />

* Used [Stayfree](https://stayfreeapps.com/) to block all forms of short-scrolling content for one month to see what would happen
* Cleaned and organized 80+ apps into clear categories using a Python script I wrote
* Ran descriptive, diagnostic, and predictive analyses in Power BI
* Built visualizations in Power BI
* Tested a small digital wellness intervention (blocking scrolls)

---

## 🎯 What I Found

### My Usage Patterns

* **YouTube and Instagram dominate** – some days 4–7 hours on these alone!
* **Weekends are worse** – about 30% higher usage than weekdays
* **Most of my phone time is "distraction"** – 63.5% of my time is pure distraction
* **Productive apps barely show up** – only a tiny 8.9% of the time

### Intervention Results (Surprising!)

I tried blocking scrolling for short-form content to see if it would help. It actually *massively* reduced my social media time! But instead of putting my phone down and being productive, my brain just found a loophole. I ended up watching longer videos on entertainment apps instead.

| Metric | September (Scroll Allowed) | October (Static - No Scroll) | Change |
| :--- | :--- | :--- | :--- |
| **Social Media Usage** | 96h | 14h | -85% ↓ |
| **Entertainment Usage** | 56h | 108h | +93% ↑ |
<img width="1270" height="718" alt="image" src="https://github.com/user-attachments/assets/3f56792f-1c55-4f52-9b2e-fa15e3886255" />

So yeah… stopping short scrolling successfully killed my social media doom scrolling, but my brain just substituted it with long-form entertainment.

---

## 📁 How I Organized the Project

```text
doom-scrolling-analysis/
├── README.md            # What you're reading now
├── data/
│   ├── raw/             # My original screen time exports
│   └── processed/       # Cleaned and ready for analysis
├── src/                 # Python scripts for cleaning & processing
└── visuals/             # Power BI visualizations
```

---

## 🔧 Tools I Used

| Task | Tool |
| :--- | :--- |
| Cleaning & Analysis | Python (Pandas, NumPy) |
| Visualizations | Power BI |
| Version Control | Git & GitHub |
| Data Collection | [Stayfree](https://stayfreeapps.com/) (Android) |

---

## Data Processing Pipeline

This pipeline:
* Cleans the raw CSV data from my Samsung A55 using [Stayfree](https://stayfreeapps.com/)
* Organizes apps into 15+ categories
* Calculates usage stats
* Outputs a file ready for Power BI

---

## What My Data Looks Like

* 1,000+ entries covering **September 1 – October 31, 2025**
* Columns include: App name, category, time spent, day, weekend flag, productivity type, etc.
* I categorized apps into: Social Media, Entertainment, Productivity, Health, Education, Games

---

## Types of Analysis I Did

1. **Descriptive:** How much I use each app, daily and weekly patterns
2. **Diagnostic:** Why my usage spikes, what days and apps are the culprits
3. **Predictive:** Guessing future patterns and high-usage days
4. **Prescriptive:** Recommendations to improve my digital wellbeing

---

## Power BI Dashboard

I built a live dashboard showing:
* Daily screen time trends
* Productivity vs distraction
* Top apps and categories (YouTube hits 9.0K minutes and Instagram hits 5.3K!)
* Weekend vs weekday patterns
* The effect of my scrolling intervention

[View My Dashboard](https://www.linkedin.com/in/abdalla-nezar-elshiekh/overlay/Project/1414659853/treasury/?profileId=ACoAAD4mNXYBjZYeJKt9vfte-amwCHgvPANOTOc)

---

## 📋 What I Learned

* **Distraction rules my phone** – 63.5% of time spent on non-productive apps
* **Behavioral substitution is real** – Cutting off scrolling just made me watch long videos instead
* **Weekends are my weak point** – 30% higher usage than weekdays
* **Late evening is my danger zone** – peak distraction time
* I want to say it's so over and I am cooked, but I never stood a chance

### My Personal Tips

* Limit total screen time to 3–4 hours per day
* Block apps during study hours (9 AM–5 PM)
* Make phone-free zones at home (it really helps a lot)
* Swap distraction apps for productivity alternatives
* Watch Mondays and weekends carefully
* Try the "5-minute rule" before opening apps

---

## 👤 About Me

**ABDALLA NEZAR ELGAILI ELSHIEKH**
BIT34503 Data Science Project
Universiti Tun Hussein Onn Malaysia (UTHM)

This project was my own attempt to understand my digital habits and take control of my phone use.

***
