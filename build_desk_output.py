import json

synopsis = ("The desk found two genuine wide-moat compounders (ADBE, KLAC) trading at credible "
            "discounts after indiscriminate AI-disruption and China-cyclical selling, plus one "
            "averaging-down opportunity in an existing core holding. Five names were watchlisted "
            "on valuation or event risk (PLTR, EXEL, ENVA, RDDT, TKO) and one was killed outright "
            "on a fresh securities-fraud class action (KD). The Hormuz ceasefire (June 17 MOU) is "
            "the dominant macro catalyst this cycle, easing the geopolitical risk premium baked "
            "into energy and freight, while China's rare-earth export curbs keep a binary cliff "
            "(Nov 10) hanging over semicap names like KLAC.")

cio = {
    "cio_view": (
        "Two names clear the bar for genuine quality at a discount: ADBE, a 37% ROIC enterprise-IP "
        "compounder beaten down on overstated AI-disruption fears, and KLAC, a wide-moat semicap "
        "monopoly trading at a 30-40% margin of safety but carrying real binary risk from China's "
        "November mineral export cliff. We are averaging down ADBE (already a core holding, now -14% "
        "from our cost basis) and initiating a half-position in KLAC to respect the China event risk "
        "while still owning the moat.\n\n"
        "Five names are watchlisted, not bought: PLTR and RDDT are priced for perfection on "
        "growth-multiple bases with no margin of safety; ENVA's headline FCF yield is a lender "
        "accounting artifact (true owner-earnings yield is 6-8%, not 46%) and its Grasshopper "
        "fintech bet is binary; EXEL just took a real Phase 3 setback in its lead pipeline asset and "
        "we hold rather than add; TKO's growth is largely inorganic (merger-driven) with ROIC too low "
        "to underwrite the current multiple.\n\n"
        "KD is killed outright: a securities class action was filed June 25, 2026 alleging internal "
        "control failures. We do not own businesses under active fraud litigation regardless of "
        "valuation — the position is uninvestable until the litigation overhang clears."
    ),
    "ranking": [
        {"rank": 1, "ticker": "ADBE", "action": "DEBATE"},
        {"rank": 2, "ticker": "KLAC", "action": "DEBATE"},
        {"rank": 3, "ticker": "EXEL", "action": "WATCHLIST"},
        {"rank": 4, "ticker": "ENVA", "action": "WATCHLIST"},
        {"rank": 5, "ticker": "PLTR", "action": "WATCHLIST"},
        {"rank": 6, "ticker": "RDDT", "action": "WATCHLIST"},
        {"rank": 7, "ticker": "TKO", "action": "WATCHLIST"},
        {"rank": 8, "ticker": "KD", "action": "KILL"},
    ],
    "best_ideas": ["ADBE", "KLAC", "ENVA"],
}

dossiers_buy = [
    {
        "ticker": "ADBE", "company": "Adobe Inc.", "verdict": "BUY", "conviction": "HIGH",
        "business": "Dominant creative-software franchise (Photoshop/Illustrator/Premiere/Acrobat) plus "
                    "a fast-growing Digital Experience (enterprise marketing/analytics) segment, sold "
                    "almost entirely via high-retention subscriptions.",
        "moat": "Enterprise IP safety/compliance moat (Firefly trained on licensed content, indemnified "
                "for commercial use) plus deep file-format and workflow lock-in across creative and "
                "marketing teams; switching costs are organizational, not just technical.",
        "thesis_check": "Market is pricing ADBE as if generic AI image/video tools structurally erode "
                         "Creative Cloud demand; we think this overstates near-term substitution risk "
                         "given enterprise IP-safety requirements and Adobe's own GenAI integration (Firefly) "
                         "inside the existing workflow.",
        "valuation": "DCF fair value ~$351/share vs ~$206 current price, a 41% margin of safety using "
                     "conservative (mid-teens) growth assumptions.",
        "margin_of_safety": "41%",
        "expected_return_3_5y": "16-18%/yr base case",
        "growth": "Low-double-digit revenue growth, high-teens EPS growth via margin expansion and buybacks.",
        "balance_sheet": "Net cash position, investment-grade, minimal leverage.",
        "catalysts": ["Firefly/GenAI monetization ramp", "Digital Experience segment re-acceleration",
                      "Multiple re-rating as AI-disruption fear proves overstated"],
        "risks": ["Genuine long-run TAM compression if generic AI tools close the enterprise-trust gap",
                  "Slower enterprise IT budget growth", "Competitive pressure from Canva/Figma in prosumer tier"],
        "what_to_watch": "Creative Cloud ARR growth reacceleration; Firefly-attributed revenue disclosure; "
                          "Digital Experience bookings",
        "entry_trigger": "Already actionable at current price ($206)",
        "time_horizon": "3-5 years",
        "thesis": "Wide-moat enterprise software compounder mispriced on overstated AI-disruption fear.",
        "proposed_by": "DeskValue", "source": "FMP fundamentals + DCF",
        "cio_one_line": "Genuine quality at a real discount — averaging down.",
        "best_idea": True, "debated": True,
        "bear_case": "If generic AI creative tools (Midjourney, Sora, etc.) close the enterprise IP-safety "
                     "and indemnification gap faster than expected, Creative Cloud's TAM compresses "
                     "structurally rather than cyclically, and no amount of Firefly integration saves the "
                     "subscription base.",
        "competitor_analysis": "Canva and Figma compete in prosumer/SMB design; neither offers enterprise "
                                "IP indemnification or Acrobat-grade document workflows. Generic AI image "
                                "generators (Midjourney, Sora) lack commercial-use licensing guarantees "
                                "enterprises require.",
        "capital_allocation_grade": "A",
        "what_would_change_our_mind": "Sustained Creative Cloud ARR deceleration below high-single-digits "
                                       "for 2+ quarters, or evidence enterprises are substituting Firefly "
                                       "with unlicensed generic AI tools at scale.",
        "deep_memo": "ADBE's bear case rests on a category error: treating consumer-grade generative AI "
                     "image tools as substitutes for an enterprise content-supply-chain platform. The real "
                     "risk is not Midjourney replacing Photoshop — it's whether Adobe successfully "
                     "monetizes Firefly fast enough to offset any seat-count deceleration. At a 41% margin "
                     "of safety against a 16-18%/yr base-case return, the market is pricing in structural "
                     "decline that the data (retention, ARR mix, Digital Experience bookings) doesn't yet "
                     "support.",
        "judge_verdict": "BUY", "judge_conviction": "HIGH",
        "judge_rationale": "Enterprise IP safety moat wins debate — bear case requires enterprises to accept "
                            "indemnification risk they've shown no appetite for.",
    },
    {
        "ticker": "PLTR", "company": "Palantir Technologies", "verdict": "WATCH", "conviction": "LOW",
        "business": "Government and commercial AI/data-analytics software platform (Gotham, Foundry, AIP).",
        "moat": "Deep government contract entrenchment and switching costs in mission-critical workflows; "
                "commercial moat far less proven.",
        "thesis_check": "Growth story is real but valuation has run far ahead of any defensible DCF.",
        "valuation": "P/S of ~59x; no margin of safety at current price.",
        "margin_of_safety": "negative", "expected_return_3_5y": "not underwritable at current price",
        "growth": "30%+ revenue growth, commercial segment accelerating.",
        "balance_sheet": "Net cash, no debt.",
        "catalysts": ["AIP commercial adoption", "Government budget expansion"],
        "risks": ["Valuation compression on any growth deceleration", "Government spending cuts"],
        "what_to_watch": "Commercial revenue mix and growth durability",
        "entry_trigger": "$55-65", "time_horizon": "watch only",
        "thesis": "Real platform, unreal price.", "proposed_by": "DeskInnovation", "source": "FMP fundamentals",
        "cio_one_line": "Priced for perfection — wait for a real pullback.",
        "best_idea": False, "debated": False, "bear_case": "Multiple compression on any growth miss.",
        "competitor_analysis": "Competes with Snowflake/Databricks in data platforms, with defense-specific "
                                "entrenchment as differentiator.",
        "capital_allocation_grade": "B",
        "what_would_change_our_mind": "Pullback to $55-65 range or evidence of durable 40%+ commercial growth.",
        "deep_memo": "", "judge_verdict": "", "judge_conviction": "", "judge_rationale": "",
    },
    {
        "ticker": "KLAC", "company": "KLA Corporation", "verdict": "WATCH", "conviction": "MEDIUM",
        "business": "Dominant process-control and yield-management equipment maker for semiconductor "
                    "fabs — effectively a monopoly in several inspection/metrology sub-markets.",
        "moat": "Process-control monopoly; switching costs for fabs are enormous (requalifying a new "
                "metrology vendor risks yield across the entire fab).",
        "thesis_check": "Quality is undeniable; the question is whether China's Nov 10 rare-earth/mineral "
                         "export cliff creates a binary hit to semicap capex that the market hasn't fully priced.",
        "valuation": "Realistic margin of safety 30-40% on a normalized-cycle DCF.",
        "margin_of_safety": "30-40%", "expected_return_3_5y": "low-teens to high-teens depending on cycle timing",
        "growth": "High-single to low-double-digit revenue growth through the cycle.",
        "balance_sheet": "Strong balance sheet, modest leverage, consistent buybacks.",
        "catalysts": ["Post-Nov-10 cliff resolution clarity", "Fab capex reacceleration (AI-driven demand)"],
        "risks": ["China Nov 10 rare-earth/mineral export cliff — binary near-term risk",
                  "Semicap capex cyclicality", "Export-control escalation"],
        "what_to_watch": "Nov 10 China mineral export cliff resolution; fab capex guidance from TSMC/Samsung/Intel",
        "entry_trigger": "Already actionable for a half-position; full position post-cliff clarity",
        "time_horizon": "3-5 years", "thesis": "Wide-moat monopoly, real but binary near-term China risk.",
        "proposed_by": "DeskValue", "source": "FMP fundamentals + DCF",
        "cio_one_line": "Buy the moat, respect the binary risk — half position now.",
        "best_idea": True, "debated": True,
        "bear_case": "If China retaliates on the Nov 10 mineral cliff with broader export restrictions on "
                     "rare earths critical to semicap manufacturing, KLAC's supply chain and Chinese "
                     "customer revenue (a meaningful % of sales) both take a hit simultaneously.",
        "competitor_analysis": "Applied Materials and ASML compete in adjacent semicap categories; KLAC's "
                                "process-control niche has the highest switching costs and fewest credible challengers.",
        "capital_allocation_grade": "A-",
        "what_would_change_our_mind": "Escalation of China mineral export curbs beyond Nov 10 baseline, "
                                       "or evidence of China developing a credible domestic process-control alternative.",
        "deep_memo": "KLAC is the highest-quality name the desk found this cycle, but it carries a real, "
                     "datable binary risk (Nov 10) that ADBE does not. The judge's call to go half-position "
                     "now and reload post-cliff is the correct way to own a wide-moat monopoly without "
                     "betting the full position on a geopolitical coin flip.",
        "judge_verdict": "WATCH", "judge_conviction": "MEDIUM",
        "judge_rationale": "Quality is clear but the Nov 10 cliff is genuinely binary — half-position respects "
                            "both the moat and the risk.",
    },
    {
        "ticker": "EXEL", "company": "Exelixis Inc.", "verdict": "WATCH", "conviction": "MEDIUM",
        "business": "Oncology biotech, lead asset cabozantinib (Cabometyx) plus pipeline including "
                    "zanzalintinib.",
        "moat": "Approved-drug royalty/revenue moat on cabozantinib; pipeline moat much weaker post-setback.",
        "thesis_check": "Already held at $51.71 cost basis (now ~$54); zanzalintinib's STELLAR-303 CRC "
                         "Phase 3 failure (June 23, 2026) removes a key pipeline catalyst.",
        "valuation": "Roughly fair value post-setback; no clear margin of safety to add.",
        "margin_of_safety": "minimal", "expected_return_3_5y": "single digits absent new pipeline data",
        "growth": "Cabometyx revenue growth moderating; pipeline growth optionality reduced.",
        "balance_sheet": "Net cash, no debt.",
        "catalysts": ["Other zanzalintinib indications still in trial", "Cabometyx label expansions"],
        "risks": ["Further pipeline setbacks", "Cabometyx generic competition timeline"],
        "what_to_watch": "Remaining zanzalintinib trial readouts in non-CRC indications",
        "entry_trigger": "hold, do not add", "time_horizon": "existing position only",
        "thesis": "Hold existing position; pipeline setback removes the add thesis.",
        "proposed_by": "DeskScreener", "source": "FMP fundamentals + clinical trial news",
        "cio_one_line": "Hold, don't add — pipeline catalyst just failed.",
        "best_idea": False, "debated": False,
        "bear_case": "Cabometyx faces eventual generic erosion with no replacement pipeline asset if "
                     "remaining trials also fail.",
        "competitor_analysis": "Competes with other RTK/VEGF inhibitors in renal and hepatocellular "
                                "carcinoma; differentiated by combination-therapy label breadth.",
        "capital_allocation_grade": "B",
        "what_would_change_our_mind": "Positive readout in a remaining zanzalintinib indication, or "
                                       "Cabometyx label expansion into a new tumor type.",
        "deep_memo": "", "judge_verdict": "", "judge_conviction": "", "judge_rationale": "",
    },
    {
        "ticker": "ENVA", "company": "Enova International", "verdict": "WATCH", "conviction": "MEDIUM",
        "business": "Non-prime consumer and small-business online lender (NetCredit, Headway Capital, "
                    "Grasshopper bank charter).",
        "moat": "Proprietary underwriting/data moat in non-prime lending; thin compared to true wide-moat names.",
        "thesis_check": "Headline 46.3% FCF yield is a lender-accounting artifact (operating cash flow "
                         "includes net loan receivable changes, which mechanically inflates OCF); true "
                         "owner-earnings yield is closer to 6-8%.",
        "valuation": "Trading at a premium to DCF ($235 implied vs $212 fair value) once FCF is normalized.",
        "margin_of_safety": "negative on normalized basis", "expected_return_3_5y": "high-single to low-double digits",
        "growth": "Loan book growing double digits; credit normalization is the swing factor.",
        "balance_sheet": "Leveraged balance sheet typical of a lender; Grasshopper bank charter is a new, unproven binary bet.",
        "catalysts": ["Grasshopper bank charter scaling", "Credit normalization post-cycle"],
        "risks": ["Grasshopper is a binary new-business bet with execution risk",
                  "Credit cycle deterioration in non-prime lending", "Headline FCF yield misleads investors into overpaying"],
        "what_to_watch": "Grasshopper deposit/loan growth trajectory; net charge-off trends",
        "entry_trigger": "$190-200 (closer to normalized DCF)", "time_horizon": "watch only",
        "thesis": "Misleading FCF yield masks a fair-to-rich valuation; Grasshopper is a binary side bet.",
        "proposed_by": "DeskContrarian", "source": "FMP fundamentals + DCF",
        "cio_one_line": "The 46% FCF yield is fake — true yield is 6-8%, and that's not cheap enough.",
        "best_idea": True, "debated": False,
        "bear_case": "Grasshopper execution stumbles while core non-prime book sees credit deterioration "
                     "in a softening labor market.",
        "competitor_analysis": "Competes with OppFi, Upstart, and traditional subprime lenders; "
                                "differentiated by proprietary underwriting data depth.",
        "capital_allocation_grade": "B-",
        "what_would_change_our_mind": "Pullback to $190-200, or Grasshopper proving out faster/cleaner than expected.",
        "deep_memo": "", "judge_verdict": "", "judge_conviction": "", "judge_rationale": "",
    },
    {
        "ticker": "RDDT", "company": "Reddit Inc.", "verdict": "WATCH", "conviction": "LOW",
        "business": "Social media/community platform monetizing via advertising and data-licensing "
                    "(AI training data deals).",
        "moat": "Network-effect moat in niche community content; data-licensing moat is new and unproven long-term.",
        "thesis_check": "Growth is real but valuation assumes flawless execution on both ad monetization "
                         "and data licensing simultaneously.",
        "valuation": "DCF margin of safety is -351% (i.e., priced ~4.5x fair value) on conservative assumptions.",
        "margin_of_safety": "-351%", "expected_return_3_5y": "not underwritable at current price",
        "growth": "30%+ revenue growth, ad load and data licensing both ramping.",
        "balance_sheet": "Net cash, no debt.",
        "catalysts": ["AI data licensing deal expansion", "International ad monetization ramp"],
        "risks": ["Multiple compression on any growth deceleration", "AI data licensing demand could plateau"],
        "what_to_watch": "Ad ARPU trends ex-US; data licensing revenue disclosure",
        "entry_trigger": "$120-130", "time_horizon": "watch only",
        "thesis": "Real growth, no margin of safety.", "proposed_by": "DeskInnovation", "source": "FMP fundamentals",
        "cio_one_line": "Wait for a real pullback — this is priced for flawless execution.",
        "best_idea": False, "debated": False, "bear_case": "Multiple compression on any growth miss.",
        "competitor_analysis": "Competes for ad dollars with Meta/Snap/Pinterest; data-licensing moat is novel "
                                "and could be competed away as other platforms ink similar deals.",
        "capital_allocation_grade": "B",
        "what_would_change_our_mind": "Pullback to $120-130.",
        "deep_memo": "", "judge_verdict": "", "judge_conviction": "", "judge_rationale": "",
    },
    {
        "ticker": "TKO", "company": "TKO Group Holdings", "verdict": "WATCH", "conviction": "LOW",
        "business": "Sports/entertainment holding company (WWE, UFC) monetizing via media rights, "
                    "live events, and sponsorships.",
        "moat": "Content/IP moat in combat sports and pro wrestling; less defensible than a true platform moat.",
        "thesis_check": "Headline growth is largely inorganic (merger-driven roll-up), and ROIC at ~6% is "
                         "too low to underwrite the current multiple.",
        "valuation": "Premium multiple not supported by organic growth or returns on capital.",
        "margin_of_safety": "minimal", "expected_return_3_5y": "high-single digits at best",
        "growth": "Headline growth largely from M&A consolidation, not organic.",
        "balance_sheet": "Elevated leverage from acquisition financing.",
        "catalysts": ["New media rights deal renewals", "International expansion"],
        "risks": ["Low ROIC limits compounding", "Leverage from M&A financing", "Media rights renewal risk"],
        "what_to_watch": "Organic (ex-M&A) revenue growth; ROIC trajectory",
        "entry_trigger": "$150-165", "time_horizon": "watch only",
        "thesis": "Inorganic growth story with ROIC too low for the multiple.",
        "proposed_by": "DeskScreener", "source": "FMP fundamentals",
        "cio_one_line": "Merger growth isn't organic growth — pass at this price.",
        "best_idea": False, "debated": False,
        "bear_case": "Media rights renewal disappoints while leverage from M&A financing limits flexibility.",
        "competitor_analysis": "Competes with other live-sports/entertainment IP holders for media-rights dollars.",
        "capital_allocation_grade": "C+",
        "what_would_change_our_mind": "Pullback to $150-165 or evidence of organic ROIC improvement.",
        "deep_memo": "", "judge_verdict": "", "judge_conviction": "", "judge_rationale": "",
    },
]

dossiers_killed = [
    {
        "ticker": "KD", "company": "Kyndryl Holdings", "verdict": "KILL", "conviction": "",
        "business": "IT infrastructure services spinoff from IBM.",
        "kill_reason": "Securities class action filed by Kuehn Law on June 25, 2026 alleging internal "
                        "control failures. We do not own businesses under active fraud litigation "
                        "regardless of valuation — uninvestable until litigation overhang clears.",
        "proposed_by": "DeskValue", "source": "FMP fundamentals + legal news",
    },
]

specialist_picks = {
    "DeskValue": [
        {"ticker": "ADBE", "thesis": "Wide-moat enterprise software compounder mispriced on overstated AI-disruption fear."},
        {"ticker": "KLAC", "thesis": "Wide-moat monopoly, real but binary near-term China risk."},
        {"ticker": "ENVA", "thesis": "Misleading FCF yield masks a fair-to-rich valuation; Grasshopper is a binary side bet."},
    ],
    "DeskInnovation": [
        {"ticker": "PLTR", "thesis": "Real platform, unreal price."},
        {"ticker": "RDDT", "thesis": "Real growth, no margin of safety."},
    ],
    "DeskContrarian": [
        {"ticker": "ENVA", "thesis": "The 46% FCF yield is fake — true yield is 6-8%."},
    ],
    "DeskScreener": [
        {"ticker": "EXEL", "thesis": "Hold, don't add — pipeline catalyst just failed."},
        {"ticker": "TKO", "thesis": "Merger growth isn't organic growth — pass at this price."},
        {"ticker": "KD", "thesis": "Killed on securities class action filed June 25, 2026."},
    ],
}

pm_decisions = {
    "portfolio_thesis": "Average down ADBE + initiate half-position KLAC as 4th tech slot.",
    "review": [
        {"ticker": "ADBE", "action": "ADD", "note": "Already held at $241.23 cost basis (-14%); adding "
         "to improve blended cost basis to ~$221.70 at the new 41% margin-of-safety discount."},
        {"ticker": "EXEL", "action": "HOLD", "note": "Pipeline setback removes the add thesis; hold existing position."},
        {"ticker": "KD", "action": "N/A", "note": "Not held; killed on class action, will not initiate."},
    ],
    "buys": [
        {"ticker": "ADBE", "action": "ADD", "shares": 48, "price": 206.00, "cost": 9888.00,
         "note": "Averages blended cost basis down to ~$221.70 across 89 total shares."},
        {"ticker": "KLAC", "action": "INITIATE", "shares": 29, "price": 278.00, "cost": 8062.00,
         "note": "Half-position to respect the China Nov 10 mineral-export cliff binary risk; reload post-cliff."},
    ],
    "starting_cash": 23163.65,
    "remaining_cash": 5213.65,
    "positions_after": "12/12 — Technology sector now FULL (QLYS, ADBE, PGY, KLAC).",
}

# ---------------------------------------------------------------------------
# Target Gap — top conviction-ranked names (conviction-adjusted upside)
# ---------------------------------------------------------------------------
target_gap = [
    {"ticker": "CMCO", "company": "Columbus McKinnon", "sector": "Industrials", "conviction": "HIGH",
     "upside_pct": 89.6, "value_driver": "revenue_target", "current_price": None,
     "why_gap_exists": "Materials-handling/crane maker trading well below stated multi-year revenue targets "
                        "post-acquisition integration overhang.",
     "risks": "Integration execution risk; industrial capex cyclicality.",
     "data_sources": "FMP fundamentals + company investor-day targets"},
    {"ticker": "PSIX", "company": "Power Solutions International", "sector": "Industrials", "conviction": "HIGH",
     "upside_pct": 84.7, "value_driver": "revenue_target",
     "why_gap_exists": "Engine/power-systems maker priced well below stated growth targets amid thin sell-side coverage.",
     "risks": "Customer concentration; cyclical end-markets.",
     "data_sources": "FMP fundamentals + company guidance"},
    {"ticker": "VREX", "company": "Vericel Corporation", "sector": "Healthcare", "conviction": "HIGH",
     "upside_pct": 75.9, "value_driver": "revenue_target",
     "why_gap_exists": "Regenerative-medicine maker trading below stated long-term revenue targets for its "
                        "cell-therapy portfolio.",
     "risks": "Reimbursement risk; clinical execution.",
     "data_sources": "FMP fundamentals + company targets"},
    {"ticker": "PTC", "company": "PTC Inc.", "sector": "Technology", "conviction": "HIGH",
     "upside_pct": 65.2, "value_driver": "revenue_target",
     "why_gap_exists": "Industrial software (CAD/PLM) name below stated ARR targets despite stable subscription transition.",
     "risks": "Industrial capex slowdown; competitive PLM market.",
     "data_sources": "FMP fundamentals + company investor day"},
    {"ticker": "REZI", "company": "Resideo Technologies", "sector": "Industrials", "conviction": "HIGH",
     "upside_pct": 64.7, "value_driver": "revenue_target",
     "why_gap_exists": "Smart-home/security products spinoff trading below stated multi-year targets.",
     "risks": "Housing-cycle sensitivity; ADI/Honeywell channel concentration.",
     "data_sources": "FMP fundamentals + company guidance"},
    {"ticker": "ACM", "company": "AECOM", "sector": "Industrials", "conviction": "HIGH",
     "upside_pct": 62.5, "value_driver": "revenue_target",
     "why_gap_exists": "Infrastructure engineering/design firm below stated backlog-to-revenue conversion targets.",
     "risks": "Government infrastructure spending risk; project execution.",
     "data_sources": "FMP fundamentals + company targets"},
    {"ticker": "ISSC", "company": "Innovative Solutions and Support", "sector": "Industrials", "conviction": "HIGH",
     "upside_pct": 57.9, "value_driver": "revenue_target",
     "why_gap_exists": "Avionics maker priced below stated growth targets amid defense/commercial aviation tailwinds.",
     "risks": "Small-cap liquidity; defense budget timing.",
     "data_sources": "FMP fundamentals + company guidance"},
    {"ticker": "TLS", "company": "Telos Corporation", "sector": "Technology", "conviction": "HIGH",
     "upside_pct": 53.2, "value_driver": "revenue_target",
     "why_gap_exists": "Cybersecurity/identity-management firm below stated multi-year targets post-restructuring.",
     "risks": "Government contract concentration; execution on restructuring.",
     "data_sources": "FMP fundamentals + company guidance"},
    {"ticker": "OPFI", "company": "OppFi Inc.", "sector": "Financial Services", "conviction": "HIGH",
     "upside_pct": 50.9, "value_driver": "revenue_target",
     "why_gap_exists": "Fintech lender below stated origination growth targets despite credit normalization.",
     "risks": "Regulatory risk on APR; credit cycle.",
     "data_sources": "FMP fundamentals + company guidance"},
    {"ticker": "NUTX", "company": "Nutex Health", "sector": "Healthcare", "conviction": "HIGH",
     "upside_pct": 38.6, "value_driver": "revenue_target",
     "why_gap_exists": "Micro-hospital operator below stated facility-expansion revenue targets.",
     "risks": "Reimbursement policy risk; facility ramp execution.",
     "data_sources": "FMP fundamentals + company guidance"},
    {"ticker": "GRND", "company": "Grindr Inc.", "sector": "Technology", "conviction": "HIGH",
     "upside_pct": 32.5, "value_driver": "revenue_target",
     "why_gap_exists": "Dating-app platform below stated subscriber/ARPU growth targets.",
     "risks": "Competitive app-store dynamics; ad/subscription mix shift.",
     "data_sources": "FMP fundamentals + company guidance"},
    {"ticker": "FIP", "company": "FTAI Infrastructure", "sector": "Industrials", "conviction": "HIGH",
     "upside_pct": 134.0, "value_driver": "asset_nav",
     "why_gap_exists": "Infrastructure asset holding company trading well below sum-of-parts asset NAV.",
     "risks": "Leverage; asset-specific operational risk.",
     "data_sources": "FMP fundamentals + asset-level NAV estimate"},
    {"ticker": "AIOT", "company": "Innodata-adjacent AI/IoT play", "sector": "Technology", "conviction": "HIGH",
     "upside_pct": 147.0, "value_driver": "asset_nav",
     "why_gap_exists": "Small-cap AI/IoT name trading well below asset-backed valuation.",
     "risks": "Thin liquidity; execution risk on AI pivot.",
     "data_sources": "FMP fundamentals"},
    {"ticker": "PICS", "company": "1stdibs.com (Pics)", "sector": "Consumer Cyclical", "conviction": "HIGH",
     "upside_pct": 104.5, "value_driver": "asset_nav",
     "why_gap_exists": "E-commerce marketplace trading well below cash + asset value.",
     "risks": "GMV growth deceleration; competitive marketplace dynamics.",
     "data_sources": "FMP fundamentals"},
    {"ticker": "GOGO", "company": "Gogo Inc.", "sector": "Technology", "conviction": "HIGH",
     "upside_pct": 171.0, "value_driver": "asset_nav",
     "why_gap_exists": "In-flight connectivity provider trading well below asset/contract-backlog value.",
     "risks": "Satellite-competitor disruption (Starlink); contract renewal risk.",
     "data_sources": "FMP fundamentals"},
    {"ticker": "BLSH", "company": "Bullish (crypto exchange)", "sector": "Financial Services", "conviction": "HIGH",
     "upside_pct": 124.9, "value_driver": "asset_nav",
     "why_gap_exists": "Crypto exchange trading well below balance-sheet asset value.",
     "risks": "Crypto-market volatility; regulatory risk.",
     "data_sources": "FMP fundamentals"},
    {"ticker": "RERE", "company": "ATRenew Inc.", "sector": "Consumer Cyclical", "conviction": "HIGH",
     "upside_pct": 79.6, "value_driver": "asset_nav",
     "why_gap_exists": "China secondhand-goods platform trading well below asset/cash value.",
     "risks": "China consumer demand; regulatory risk.",
     "data_sources": "FMP fundamentals"},
    {"ticker": "ADMA", "company": "ADMA Biologics", "sector": "Healthcare", "conviction": "MEDIUM",
     "upside_pct": 91.4, "value_driver": "revenue_target",
     "why_gap_exists": "Plasma-derived biologics maker below stated production-capacity revenue targets.",
     "risks": "Plasma supply chain; manufacturing scale-up execution.",
     "data_sources": "FMP fundamentals + company guidance"},
    {"ticker": "GPOR", "company": "Gulfport Energy", "sector": "Energy", "conviction": "MEDIUM",
     "upside_pct": 26.5, "value_driver": "revenue_target",
     "why_gap_exists": "Natural-gas E&P below stated production-growth targets amid gas-price volatility.",
     "risks": "Natural gas price volatility; hedging program effectiveness.",
     "data_sources": "FMP fundamentals + company guidance"},
    {"ticker": "CLS", "company": "Celestica Inc.", "sector": "Technology", "conviction": "MEDIUM",
     "upside_pct": 20.6, "value_driver": "revenue_target",
     "why_gap_exists": "Electronics manufacturing services name below stated AI-datacenter revenue targets.",
     "risks": "Customer concentration; margin pressure in EMS industry.",
     "data_sources": "FMP fundamentals + company guidance"},
    {"ticker": "STX", "company": "Seagate Technology", "sector": "Technology", "conviction": "LOW",
     "upside_pct": -0.4, "value_driver": "revenue_target", "current_price": 901.50,
     "why_gap_exists": "Price appears to reflect a data anomaly (likely post-split adjustment) — flagged, "
                        "not actionable.",
     "risks": "Data quality flag — verify split adjustment before any action.",
     "data_sources": "FMP fundamentals (flagged anomaly)"},
    {"ticker": "CLMB", "company": "Climb Global Solutions", "sector": "Technology", "conviction": "LOW",
     "upside_pct": 449.0, "value_driver": "revenue_target",
     "why_gap_exists": "Upside figure reflects stale analyst targets vs a stock that has already re-rated "
                        "sharply higher — data anomaly, not a real opportunity.",
     "risks": "Data quality flag — analyst targets are stale.",
     "data_sources": "FMP fundamentals (flagged anomaly)"},
    {"ticker": "MVST", "company": "Microvast Holdings", "sector": "Industrials", "conviction": "LOW",
     "upside_pct": 475.0, "value_driver": "revenue_target",
     "why_gap_exists": "Stock collapsed from $7 to $1.13 on a revenue miss with going-concern risk; the "
                        "headline upside is not a real opportunity.",
     "risks": "Going-concern risk; revenue miss; do not treat as actionable.",
     "data_sources": "FMP fundamentals (flagged going-concern risk)"},
]

# ---------------------------------------------------------------------------
# Strategist (geopolitics) desk output
# ---------------------------------------------------------------------------
geopolitics = {
    "house_view": {
        "stance": "BALANCED",
        "one_liner": ("The Hormuz ceasefire is the single biggest macro event of 2026 — the June 17 "
                      "US-Iran MOU ending a 4-month Strait of Hormuz closure removes the dominant risk "
                      "premium from energy and freight, but China's Nov 10 rare-earth/mineral export "
                      "cliff keeps a second binary event on the calendar for semicap and EV-supply-chain names."),
        "conviction": "MEDIUM",
        "key_swing_factors": [
            "Durability of the Hormuz ceasefire through year-end",
            "Whether China escalates or holds the line on the Nov 10 mineral export cliff",
            "Fed policy path given a still-elevated 10Y (4.4%)",
            "Russia-Ukraine ceasefire negotiation progress",
        ],
        "best_bets": [
            {"idea": "Short tanker/freight rates normalization (e.g. product tanker operators)",
             "direction": "short", "thesis": "Hormuz ceasefire removes the war-risk premium baked into "
             "freight rates since the closure began.", "odds": "~60%", "time_horizon": "3-6mo",
             "risk": "Ceasefire breaks down, freight premium returns"},
            {"idea": "Long gold as a geopolitical/inflation hedge", "direction": "long",
             "thesis": "Gold at $4,041 reflects ongoing macro uncertainty; China mineral cliff and Fed "
             "policy ambiguity both support continued demand.", "odds": "~55%", "time_horizon": "6-12mo",
             "risk": "Sharp risk-on rally compresses safe-haven demand"},
            {"idea": "Long European/regional financials", "direction": "long",
             "thesis": "Hormuz de-escalation lowers energy-import cost pressure on European economies, "
             "supporting credit growth and bank earnings.", "odds": "~50%", "time_horizon": "6-12mo",
             "risk": "ECB policy tightens faster than expected"},
            {"idea": "Selective long energy majors on dips, not chase the rally", "direction": "long",
             "thesis": "Oil prices (Brent $73.97) have already priced in much of the ceasefire relief; "
             "majors remain reasonably valued on normalized long-run prices.", "odds": "~50%", "time_horizon": "6-12mo",
             "risk": "OPEC+ supply discipline breaks down"},
            {"idea": "Avoid semicap/China-exposed names ahead of Nov 10 cliff (or size to half-position)",
             "direction": "avoid/reduce", "thesis": "Binary risk event with asymmetric downside for "
             "China-revenue-exposed semicap suppliers.", "odds": "~50% escalation risk", "time_horizon": "now-Nov 2026",
             "risk": "Missing upside if China backs down without incident"},
            {"idea": "Long defense/aerospace on Taiwan/Russia-Ukraine tail risk", "direction": "long",
             "thesis": "Elevated geopolitical tail risk across two theaters supports continued defense budget growth.",
             "odds": "~55%", "time_horizon": "12mo+", "risk": "Major de-escalation reduces budget urgency"},
            {"idea": "Long US regional banks on rate-environment stability", "direction": "long",
             "thesis": "10Y stabilizing around 4.4% removes the duration-mismatch stress regional banks faced in prior cycles.",
             "odds": "~50%", "time_horizon": "6-12mo", "risk": "Renewed rate volatility"},
            {"idea": "Short/avoid EV-supply-chain names most exposed to China rare-earth curbs",
             "direction": "short/avoid", "thesis": "Magnet and battery-material supply chains are most "
             "exposed to a China mineral export escalation.", "odds": "~45%", "time_horizon": "now-Nov 2026",
             "risk": "China holds the line, no escalation materializes"},
            {"idea": "Long select industrials/infrastructure on de-risked global trade flows",
             "direction": "long", "thesis": "Hormuz ceasefire plus stable USDCNY supports a more normal "
             "global trade/shipping backdrop.", "odds": "~50%", "time_horizon": "6-12mo",
             "risk": "Tariff escalation reverses trade normalization"},
            {"idea": "Barbell: own quality compounders (ADBE-type) + a gold/defense hedge sleeve",
             "direction": "long", "thesis": "Balanced stance argues for owning quality at a discount while "
             "carrying tail-risk hedges given two live binary events.", "odds": "n/a — portfolio construction call",
             "time_horizon": "ongoing", "risk": "Hedge sleeve drags returns if both situations de-escalate cleanly"},
        ],
    },
    "situations": [
        {
            "name": "Strait of Hormuz / Iran",
            "status": "de-escalating",
            "scenarios": [
                {"label": "base", "odds": "55%",
                 "summary": "June 17 US-Iran MOU holds; Hormuz traffic normalizes over Q3 2026; oil and "
                             "freight risk premiums continue unwinding gradually.",
                 "winners": ["Airlines", "Global freight/logistics", "Oil importers (EU, Japan, India)"],
                 "losers": ["Tanker operators", "Oil majors (lower realized prices)", "Defense names tied to Gulf tension"]},
                {"label": "escalation", "odds": "20%",
                 "summary": "MOU breaks down on a triggering incident (proxy attack, inspection dispute); "
                             "Hormuz traffic disruption resumes, oil spikes.",
                 "winners": ["Tanker operators", "Oil majors", "Gold", "Defense primes"],
                 "losers": ["Airlines", "Global freight", "Oil-importing economies", "Risk assets broadly"]},
                {"label": "de-escalation", "odds": "25%",
                 "summary": "Durable diplomatic resolution extends beyond the MOU into a broader regional "
                             "framework; risk premium fully unwinds.",
                 "winners": ["Airlines", "Freight/logistics", "EU/Asia oil importers", "Risk assets broadly"],
                 "losers": ["Tanker operators", "Gold", "Defense names tied to Gulf tension"]},
            ],
            "consequences_3_6mo": "Continued gradual unwind of the energy/freight risk premium baked in "
                                   "since the closure began; watch for any incident that could re-trigger escalation.",
            "watch": "Inspection-regime compliance milestones; any proxy-group attacks in the Gulf",
            "sources": "web (labeled, verify) + FMP commodity/FX prices",
        },
        {
            "name": "China critical-mineral export curbs",
            "status": "escalating",
            "scenarios": [
                {"label": "base", "odds": "45%",
                 "summary": "Current restrictions hold through Nov 10 deadline without further escalation; "
                             "semicap/EV supply chains absorb elevated input costs but avoid acute shortage.",
                 "winners": ["Non-China rare-earth/mineral producers", "Domestic semicap supply-chain alternatives"],
                 "losers": ["China-revenue-exposed semicap (KLAC, AMAT, ASML)", "EV battery/magnet supply chains"]},
                {"label": "escalation", "odds": "35%",
                 "summary": "China expands export curbs at or before the Nov 10 cliff in retaliation for "
                             "further US export controls; acute shortages hit semicap and EV magnet supply chains.",
                 "winners": ["Non-China rare-earth producers", "Gold", "Defense names"],
                 "losers": ["KLAC and China-revenue-exposed semicap broadly", "EV/battery supply chains", "Risk assets in tech"]},
                {"label": "de-escalation", "odds": "20%",
                 "summary": "US-China trade talks produce a partial rollback of mineral export restrictions "
                             "ahead of the Nov 10 deadline.",
                 "winners": ["KLAC and semicap broadly", "EV/battery supply chains", "Risk assets in tech"],
                 "losers": ["Non-China rare-earth producers (less urgency)", "Gold"]},
            ],
            "consequences_3_6mo": "The Nov 10 cliff is the key datable catalyst; semicap names with China "
                                   "revenue exposure (notably KLAC) carry real binary risk into year-end.",
            "watch": "Nov 10, 2026 export-curb deadline; any US export-control escalation that could trigger Chinese retaliation",
            "sources": "web (labeled, verify) + FMP commodity/FX prices",
        },
        {
            "name": "Russia-Ukraine",
            "status": "stable",
            "scenarios": [
                {"label": "base", "odds": "50%",
                 "summary": "Frozen conflict continues with periodic ceasefire negotiation attempts but no breakthrough.",
                 "winners": ["Defense primes", "European energy-independence plays"],
                 "losers": ["European reconstruction-exposed names (delayed)", "Russia-adjacent commodity flows"]},
                {"label": "escalation", "odds": "15%",
                 "summary": "Renewed offensive action disrupts negotiations; European energy security concerns resurface.",
                 "winners": ["Defense primes", "Gold", "European energy-independence plays"],
                 "losers": ["European risk assets broadly", "Global risk sentiment"]},
                {"label": "de-escalation", "odds": "35%",
                 "summary": "Ceasefire negotiations make tangible progress, opening a path to eventual "
                             "reconstruction and reduced European defense-spending urgency.",
                 "winners": ["European reconstruction-exposed industrials", "Risk assets broadly"],
                 "losers": ["Defense primes (slower budget growth)", "Gold"]},
            ],
            "consequences_3_6mo": "Largely a background risk factor; no near-term datable catalyst expected "
                                   "to shift the base case materially.",
            "watch": "Any formal ceasefire negotiation announcements",
            "sources": "web (labeled, verify)",
        },
        {
            "name": "Taiwan / Semiconductors",
            "status": "stable",
            "scenarios": [
                {"label": "base", "odds": "65%",
                 "summary": "Status quo holds; Taiwan semicon supply chain continues operating normally "
                             "with periodic rhetorical tension.",
                 "winners": ["TSMC and Taiwan semicon supply chain", "AI-infrastructure capex beneficiaries"],
                 "losers": ["Risk-premium-sensitive Taiwan-adjacent names"]},
                {"label": "escalation", "odds": "15%",
                 "summary": "Heightened military posturing or incident raises supply-chain disruption concerns "
                             "without an actual blockade.",
                 "winners": ["Non-Taiwan semicon alternatives (limited)", "Gold", "Defense"],
                 "losers": ["TSMC-dependent supply chains broadly", "Global tech risk sentiment"]},
                {"label": "de-escalation", "odds": "20%",
                 "summary": "Diplomatic engagement reduces tension; status quo reaffirmed with reduced rhetoric.",
                 "winners": ["Taiwan semicon supply chain", "Global tech risk sentiment"],
                 "losers": ["Defense/hedge positioning tied to Taiwan risk"]},
            ],
            "consequences_3_6mo": "Background tail risk; the China mineral-curb situation is the more "
                                   "immediate, datable semicap risk factor right now.",
            "watch": "Any change in cross-strait military posturing or US-Taiwan policy signals",
            "sources": "web (labeled, verify)",
        },
        {
            "name": "US Tariffs / Trade Policy",
            "status": "stable",
            "scenarios": [
                {"label": "base", "odds": "55%",
                 "summary": "Existing tariff regime holds with no major new escalation; USDCNY remains "
                             "relatively stable (6.7849).",
                 "winners": ["Domestic-focused industrials", "Companies with diversified non-China supply chains"],
                 "losers": ["China-import-dependent retailers/manufacturers"]},
                {"label": "escalation", "odds": "25%",
                 "summary": "New tariff actions tied to the mineral-export dispute or other trade friction "
                             "points raise costs across import-dependent sectors.",
                 "winners": ["Domestic manufacturers", "Tariff-exempt supply-chain alternatives"],
                 "losers": ["China-import-dependent retailers", "Global trade-sensitive risk assets"]},
                {"label": "de-escalation", "odds": "20%",
                 "summary": "Trade talks produce tariff relief alongside any mineral-export resolution.",
                 "winners": ["China-import-dependent retailers", "Global trade-sensitive risk assets"],
                 "losers": ["Domestic-protected manufacturers (relatively)"]},
            ],
            "consequences_3_6mo": "Tariff policy is increasingly intertwined with the China mineral-export "
                                   "situation — watch them together rather than independently.",
            "watch": "Any new tariff announcements tied to the Nov 10 mineral cliff negotiations",
            "sources": "web (labeled, verify) + FMP FX data",
        },
    ],
}

sector_rotation = {
    "regime_read": ("A late-cycle, AI-infrastructure-led expansion running into policy friction — sector "
                    "leadership is bifurcating between AI-capex beneficiaries (still expensive) and "
                    "geopolitically-discounted value pockets (financials, real estate, select industrials) "
                    "that screen cheap on a relative basis."),
    "sectors": [
        {"sector": "Financial Services", "valuation": "cheap", "trend": "Hormuz de-escalation and stable "
         "rate environment support credit growth without duration stress.", "verdict": "accumulate",
         "why": "PEG ~1.0, attractively priced relative to growth, benefits from both rate stability and "
         "easing geopolitical risk premium.", "odds_attractive": "~65%"},
        {"sector": "Communication Services", "valuation": "fair", "trend": "Ad-revenue normalization plus "
         "AI-data-licensing optionality (Reddit-style deals) creating a new monetization layer.",
         "verdict": "accumulate", "why": "PEG ~1.6, growth re-acceleration not fully priced in.", "odds_attractive": "~60%"},
        {"sector": "Real Estate", "valuation": "cheap", "trend": "Data-center demand from AI infrastructure "
         "buildout is a structural tailwind for a sector still pricing in higher-for-longer rates.",
         "verdict": "accumulate", "why": "Data-center REIT subsegment particularly mispriced relative to "
         "AI-infrastructure capex growth.", "odds_attractive": "~60%"},
        {"sector": "Technology", "valuation": "rich", "trend": "AI capex still driving the mega-cap/semicap "
         "leadership, but binary China mineral-export risk (Nov 10) creates real dispersion within the sector.",
         "verdict": "hold (selective)", "why": "Only semicap names with limited China exposure or genuine "
         "moat discounts (ADBE, KLAC at MoS) are attractive; broad-sector multiples remain rich.",
         "odds_attractive": "~40%"},
        {"sector": "Industrials", "valuation": "fair", "trend": "Defense/power-infrastructure demand "
         "offsetting cyclical capex softness; trade-normalization tailwind from Hormuz de-escalation.",
         "verdict": "accumulate (selective)", "why": "Defense and power-infrastructure subsegments most "
         "attractive; broad industrials fairly valued.", "odds_attractive": "~55%"},
        {"sector": "Energy", "valuation": "fair", "trend": "Oil prices already reflect much of the Hormuz "
         "ceasefire relief; majors reasonably valued on normalized long-run prices.", "verdict": "hold",
         "why": "Limited further re-rating upside absent OPEC+ supply discipline changes.", "odds_attractive": "~45%"},
        {"sector": "Healthcare", "valuation": "fair", "trend": "Idiosyncratic pipeline-driven dispersion "
         "(e.g. EXEL setback) rather than sector-wide repricing.", "verdict": "hold",
         "why": "Stock-picker's market — no clear sector-wide valuation signal.", "odds_attractive": "~45%"},
        {"sector": "Consumer Cyclical", "valuation": "fair", "trend": "Consumer spending resilience offset "
         "by tariff-policy uncertainty.", "verdict": "hold", "why": "Mixed signals, no clear sector edge.",
         "odds_attractive": "~45%"},
        {"sector": "Consumer Defensive", "valuation": "rich", "trend": "Defensive premium overpaid for given "
         "the de-escalating geopolitical backdrop reduces the need for ballast.", "verdict": "avoid",
         "why": "Priced for a risk-off regime that the Hormuz ceasefire argues against.", "odds_attractive": "~25%"},
        {"sector": "Utilities", "valuation": "rich", "trend": "Bond-proxy premium overextended given the "
         "10Y stabilizing around 4.4% rather than falling further.", "verdict": "avoid",
         "why": "Priced for falling rates that the current Fed-neutral stance doesn't support.", "odds_attractive": "~25%"},
        {"sector": "Materials", "valuation": "fair", "trend": "China mineral-export curbs create dispersion "
         "between non-China producers (beneficiaries) and China-supply-chain-dependent names (at risk).",
         "verdict": "hold (selective)", "why": "Non-China rare-earth/mineral producers are the selective "
         "opportunity within an otherwise fairly-valued sector.", "odds_attractive": "~50%"},
        {"sector": "Basic Materials/Industrial Metals", "valuation": "fair", "trend": "Copper/silver strength "
         "reflects both AI-infrastructure electrification demand and safe-haven flows.", "verdict": "hold",
         "why": "Already reflects much of the bullish electrification thesis.", "odds_attractive": "~45%"},
    ],
    "top_opportunities": [
        "Financial Services — PEG ~1.0, benefits from both rate stability and easing geopolitical risk premium",
        "Communication Services — AI-data-licensing optionality not fully priced into PEG ~1.6",
        "Real Estate (data-center REIT subsegment) — structural AI-infrastructure tailwind still mispriced",
        "Industrials (defense/power-infrastructure subsegment) — two live geopolitical tail risks support continued budget growth",
        "Technology (semicap with limited China exposure or genuine moat discount, e.g. ADBE/KLAC) — sector broadly rich but real dispersion creates selective opportunity",
    ],
    "risk_on_or_wait": ("Take selective risk now — the Hormuz de-escalation argues against paying a "
                        "defensive-sector premium, but size positions to respect the still-live China "
                        "mineral-export binary risk (Nov 10) rather than going broadly risk-on."),
}

market_outlook = {
    "near_term_bias": "neutral",
    "long_term_bias": "bullish",
    "crash_risk": "ELEVATED",
    "rationale": ("Near-term bias is neutral given two live binary geopolitical catalysts (China mineral "
                 "cliff Nov 10, Hormuz ceasefire durability) that could swing risk sentiment sharply in "
                 "either direction. Long-term bias remains bullish on AI-infrastructure-led productivity "
                 "growth and a de-escalating geopolitical backdrop. Crash risk is elevated specifically "
                 "because of the binary nature of the Nov 10 China mineral cliff — a clean resolution "
                 "supports the bullish long-term case, but an escalation could trigger a sharp, "
                 "concentrated drawdown in semicap/tech."),
}

macro_dashboard = {
    "recession_risk": "MODERATE",
    "fed_policy": "NEUTRAL",
    "rate_environment": "10Y at approximately 4.4%, stabilizing after the prior cycle's volatility; Fed "
                        "policy stance is neutral, neither actively tightening nor signaling imminent cuts.",
}

macro_context = ("The June 17 US-Iran MOU ending the 4-month Hormuz closure is the macro story of 2026 — "
                  "it has begun unwinding the energy and freight risk premium that built up over the "
                  "closure period, with Brent crude at $73.97 (-1.71%) reflecting some of that relief "
                  "already. Gold at $4,041 (-1.35%) and silver at $58.63 (-1.75%) remain elevated, "
                  "signaling markets are still pricing meaningful macro/geopolitical uncertainty rather "
                  "than an all-clear. The next major datable catalyst is China's Nov 10, 2026 rare-earth "
                  "and critical-mineral export cliff, which carries real binary risk for semicap and "
                  "EV-supply-chain names with China revenue exposure. FX markets (EUR/USD 1.1426, "
                  "USD/CNY 6.7849, USD/JPY 161.94) show no acute stress signal yet, consistent with a "
                  "balanced, wait-and-see macro regime rather than outright risk-off.")

output = {
    "synopsis": synopsis,
    "cio": cio,
    "dossiers_buy": dossiers_buy,
    "dossiers_killed": dossiers_killed,
    "specialist_picks": specialist_picks,
    "pm_decisions": pm_decisions,
    "target_gap": target_gap,
    "geopolitics": geopolitics,
    "sector_rotation": sector_rotation,
    "market_outlook": market_outlook,
    "macro_dashboard": macro_dashboard,
    "macro_context": macro_context,
}

with open("desk_output.json", "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print("Wrote desk_output.json:", len(json.dumps(output)), "bytes")
print("Keys:", list(output.keys()))
print("dossiers_buy:", [d["ticker"] for d in dossiers_buy])
print("target_gap count:", len(target_gap))
