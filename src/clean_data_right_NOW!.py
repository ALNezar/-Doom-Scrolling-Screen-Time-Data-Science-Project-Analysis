"""
================================================================================
SCREEN TIME DATA ETL PIPELINE 
================================================================================
BIT34503 Data Science Project - UTHM
Transforms raw screen time CSV exports into analysis-ready dataset for Power BI

Features:
- Robust time parsing with validation and error handling
- Duplicate app entry merging (sums usage per app per day)
- Comprehensive app categorization (~80+ apps mapped)
- Power BI-ready derived columns for analytics
- Date column formatted as YYYY-MM-DD (numeric date for Power BI)
- Data quality reporting

Output:
- final_cleaned_data.csv : Complete analysis-ready dataset for Power BI

===================================================================
"""

import pandas as pd
import numpy as np
import re
import os
import logging
from datetime import datetime
from typing import Tuple, Optional, List, Dict
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

# Input files configuration
INPUT_FILES: List[Dict] = [
    {
        "filename": "September_data.csv",
        "month_filter": "September",
        "interaction_mode": "Scroll Allowed"
    },
    {
        "filename": "October_data.csv", 
        "month_filter": "October",
        "interaction_mode": "Static (No Scroll)"
    }
]

# Output file (single consolidated output)
OUTPUT_FILE = "final_cleaned_data.csv"

# Usage tier thresholds (in minutes)
USAGE_TIERS = {
    "Low": (0, 15),
    "Medium": (15, 60),
    "High": (60, 120),
    "Very High": (120, float('inf'))
}

# ============================================================================
# APP CATEGORIZATION MAPPINGS
# ============================================================================

APP_CATEGORIES: Dict[str, str] = {
    # Social Media
    'instagram': 'Social Media',
    'instagram.com': 'Social Media',
    'tiktok': 'Social Media',
    'twitter': 'Social Media',
    'x': 'Social Media',
    'facebook': 'Social Media',
    'snapchat': 'Social Media',
    'reddit': 'Social Media',
    'reddit.com': 'Social Media',
    'threads': 'Social Media',
    'linkedin': 'Social Media',
    
    # Communication
    'whatsapp': 'Communication',
    'telegram': 'Communication',
    'discord': 'Communication',
    'slack': 'Communication',
    'messages': 'Communication',
    'imo': 'Communication',
    'call': 'Communication',
    'phone': 'Communication',
    
    # Entertainment - Video
    'youtube': 'Entertainment',
    'youtube.com': 'Entertainment',
    'netflix': 'Entertainment',
    'twitch': 'Entertainment',
    'yt studio': 'Entertainment',
    
    # Entertainment - Music
    'spotify': 'Entertainment',
    'soundcore': 'Entertainment',
    
    # Entertainment - Reading
    'tachiyomi': 'Entertainment',
    'esentral': 'Entertainment',
    
    # Games
    'roblox': 'Games',
    'roblox.com': 'Games',
    'create.roblox.com': 'Games',
    'doodle jump': 'Games',
    'wordle': 'Games',
    'subway surf': 'Games',
    'solar smash': 'Games',
    'tetris': 'Games',
    'light it up': 'Games',
    'scratchjr': 'Games',
    'game': 'Games',
    
    # Productivity & Work
    'notion': 'Productivity',
    'focus to-do': 'Productivity',
    'focus': 'Productivity',
    'planwiz': 'Productivity',
    'keep notes': 'Productivity',
    'samsung notes': 'Productivity',
    'cs pdf': 'Productivity',
    'calendar': 'Productivity',
    'sheets': 'Productivity',
    'excel': 'Productivity',
    
    # Development & AI
    'github': 'Development',
    'github.com': 'Development',
    'vs code': 'Development',
    'python': 'Development',
    'chatgpt': 'AI Tools',
    'claude': 'AI Tools',
    'copilot': 'AI Tools',
    
    # Education
    'coursera': 'Education',
    'coursera.org': 'Education',
    'edx.org': 'Education',
    'theodinproject.com': 'Education',
    'myuthm': 'Education',
    'study.uthm.edu.my': 'Education',
    'amo.uthm.edu.my': 'Education',
    'author.uthm.edu.my': 'Education',
    
    # Browsers
    'chrome': 'Browser',
    'firefox': 'Browser',
    'firefox nightly': 'Browser',
    'safari': 'Browser',
    'samsung internet': 'Browser',
    
    # Shopping
    'lazada': 'Shopping',
    'shopee': 'Shopping',
    'amazon': 'Shopping',
    'redbus': 'Shopping',
    
    # Finance
    'mae': 'Finance',
    'bank': 'Finance',
    'wallet': 'Finance',
    
    # Health & Wellness
    'samsung health': 'Health',
    'lifesum': 'Health',
    'digital wellbeing': 'Health',
    'stayfree': 'Health',
    
    # Photography & Media
    'camera': 'Media',
    'gallery': 'Media',
    'photos': 'Media',
    'photo editor': 'Media',
    'photos & videos': 'Media',
    'ibispaint': 'Media',
    'faceapp': 'Media',
    'magic eraser': 'Media',
    'ar emoji': 'Media',
    'edits': 'Media',
    
    # Cloud & Storage
    'drive': 'Cloud Storage',
    'google drive': 'Cloud Storage',
    'files': 'Cloud Storage',
    
    # Email
    'gmail': 'Email',
    'mail': 'Email',
    
    # Utilities
    'calculator': 'Utilities',
    'clock': 'Utilities',
    'weather': 'Utilities',
    'translate': 'Utilities',
    'maps': 'Utilities',
    'authenticator': 'Utilities',
    'applock': 'Utilities',
    'contacts': 'Utilities',
    'quick share': 'Utilities',
    'shareit': 'Utilities',
    
    # System
    'one ui home': 'System',
    'system ui': 'System',
    'settings': 'System',
    'launcher': 'System',
    'package installer': 'System',
    'permission controller': 'System',
    'android system': 'System',
    'device care': 'System',
    'google play': 'System',
    'galaxy store': 'System',
    'credential manager': 'System',
    'autofill': 'System',
    'link to windows': 'System',
    'smartthings': 'System',
    'captive portal': 'System',
    'intentresolver': 'System',
    
    # Search & Info
    'google': 'Search',
    'google.com': 'Search',
    'wikipedia': 'Search',
    'quora.com': 'Search',
    
    # Government Services
    'absher': 'Government',
    'visa.educationmalaysia': 'Government',
    'celcom life': 'Telecom',
}

# Productivity classification for each category
PRODUCTIVITY_MAP: Dict[str, str] = {
    'Social Media': 'Distraction',
    'Entertainment': 'Distraction',
    'Games': 'Distraction',
    'Shopping': 'Distraction',
    
    'Development': 'Productive',
    'Productivity': 'Productive',
    'AI Tools': 'Productive',
    'Education': 'Productive',
    
    'Health': 'Positive Habit',
    'Email': 'Neutral',
    'Communication': 'Neutral',
    'Browser': 'Neutral',
    'System': 'Neutral',
    'Utilities': 'Neutral',
    'Media': 'Neutral',
    'Cloud Storage': 'Neutral',
    'Search': 'Neutral',
    'Finance': 'Neutral',
    'Government': 'Neutral',
    'Telecom': 'Neutral',
}

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CORE FUNCTIONS
# ============================================================================

def parse_time_to_minutes(time_str: str) -> float:
    """
    Parse time string (e.g., '1h 30m 45s', '45m', '30s') to decimal minutes.
    
    Handles various formats:
    - Hours, minutes, seconds: '2h 30m 15s'
    - Minutes and seconds: '45m 30s'
    - Only minutes: '45m'
    - Only seconds: '30s'
    - Zero values: '0s', '0', ''
    
    Args:
        time_str: Time string to parse
        
    Returns:
        Duration in decimal minutes (float), or 0.0 if parsing fails
    """
    if pd.isna(time_str):
        return 0.0
    
    s = str(time_str).strip().lower()
    
    # Handle zero/empty cases
    if s in ['0', '0s', '', 'nan', 'none', '-']:
        return 0.0
    
    try:
        hours = 0
        minutes = 0
        seconds = 0
        
        # Extract hours
        h_match = re.search(r'(\d+)\s*h', s)
        if h_match:
            hours = int(h_match.group(1))
        
        # Extract minutes
        m_match = re.search(r'(\d+)\s*m(?!s)', s)  # 'm' not followed by 's' (to avoid 'ms')
        if m_match:
            minutes = int(m_match.group(1))
        
        # Extract seconds
        s_match = re.search(r'(\d+)\s*s', s)
        if s_match:
            seconds = int(s_match.group(1))
        
        total_minutes = (hours * 60) + minutes + (seconds / 60)
        return round(total_minutes, 2)
        
    except Exception as e:
        logger.warning(f"Failed to parse time string '{time_str}': {e}")
        return 0.0


def get_category_info(app_name: str) -> Tuple[str, str]:
    """
    Get category and productivity type for an app.
    
    Uses longest-match strategy to handle cases like 'instagram.com' 
    matching 'instagram.com' over 'instagram'.
    
    Args:
        app_name: Name of the application
        
    Returns:
        Tuple of (category, productivity_type)
    """
    if pd.isna(app_name):
        return ("Other", "Neutral")
    
    name_lower = str(app_name).lower().strip()
    
    # Find best match (longest matching keyword wins)
    best_match_category = None
    max_len = 0
    
    for keyword, category in APP_CATEGORIES.items():
        if keyword in name_lower:
            if len(keyword) > max_len:
                max_len = len(keyword)
                best_match_category = category
    
    if best_match_category:
        productivity = PRODUCTIVITY_MAP.get(best_match_category, "Neutral")
        return (best_match_category, productivity)
    
    return ("Other", "Neutral")


def get_usage_tier(minutes: float) -> str:
    """
    Classify usage duration into tiers for Power BI analysis.
    
    Args:
        minutes: Duration in minutes
        
    Returns:
        Usage tier label (Low/Medium/High/Very High)
    """
    for tier, (min_val, max_val) in USAGE_TIERS.items():
        if min_val <= minutes < max_val:
            return tier
    return "Very High"


def validate_dataframe(df: pd.DataFrame, filename: str) -> bool:
    """
    Validate that DataFrame has required structure.
    
    Args:
        df: DataFrame to validate
        filename: Source filename for error messages
        
    Returns:
        True if valid, False otherwise
    """
    if df.empty:
        logger.error(f"{filename}: DataFrame is empty")
        return False
    
    if len(df.columns) < 3:
        logger.error(f"{filename}: Insufficient columns (found {len(df.columns)})")
        return False
    
    return True


def process_single_file(
    filename: str,
    month_filter: str,
    interaction_mode: str
) -> Optional[pd.DataFrame]:
    """
    Process a single CSV file and return cleaned DataFrame.
    
    Args:
        filename: Path to CSV file
        month_filter: Month name to filter columns (e.g., 'September')
        interaction_mode: Interaction mode label
        
    Returns:
        Processed DataFrame or None if error
    """
    if not os.path.exists(filename):
        logger.warning(f"File not found: {filename}")
        return None
    
    try:
        logger.info(f"Processing: {filename}")
        
        # Read CSV
        df = pd.read_csv(filename)
        
        if not validate_dataframe(df, filename):
            return None
        
        # Rename first column to 'App'
        df.rename(columns={df.columns[0]: 'App'}, inplace=True)
        
        # Remove metadata rows
        df = df[~df['App'].isin(['Total Usage', '', np.nan])]
        df = df[df['App'].notna()]
        
        # Keep Device column if exists, otherwise create placeholder
        device_col = 'Device' if 'Device' in df.columns else None
        
        # Filter columns for specific month
        date_cols = [c for c in df.columns if month_filter in str(c)]
        
        if not date_cols:
            logger.warning(f"{filename}: No columns found for {month_filter}")
            return None
        
        cols_to_keep = ['App']
        if device_col:
            cols_to_keep.append(device_col)
        cols_to_keep.extend(date_cols)
        
        df = df[cols_to_keep]
        
        # Melt (unpivot) from wide to long format
        id_vars = ['App'] + ([device_col] if device_col else [])
        df_melted = df.melt(
            id_vars=id_vars,
            var_name='Date',
            value_name='Usage_Raw'
        )
        
        # Add metadata columns
        df_melted['Interaction_Mode'] = interaction_mode
        df_melted['Source_File'] = filename
        
        logger.info(f"  -> Extracted {len(df_melted)} rows from {len(date_cols)} date columns")
        
        return df_melted
        
    except Exception as e:
        logger.error(f"Error processing {filename}: {e}")
        return None


def merge_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge duplicate app entries by summing usage per app per day.
    
    Args:
        df: DataFrame with potential duplicates
        
    Returns:
        DataFrame with duplicates merged
    """
    logger.info("Merging duplicate app entries...")
    
    # Count duplicates before merge
    dup_count = df.duplicated(subset=['App', 'Date'], keep=False).sum()
    
    if dup_count > 0:
        logger.info(f"  -> Found {dup_count} duplicate entries to merge")
    
    # Group by App and Date, sum duration, keep first of other columns
    agg_dict = {
        'Duration_Minutes': 'sum',
        'Usage_Raw': 'first',
        'Interaction_Mode': 'first',
        'Source_File': 'first',
    }
    
    # Add Device if present
    if 'Device' in df.columns:
        agg_dict['Device'] = 'first'
    
    df_merged = df.groupby(['App', 'Date'], as_index=False).agg(agg_dict)
    
    rows_reduced = len(df) - len(df_merged)
    if rows_reduced > 0:
        logger.info(f"  -> Reduced from {len(df)} to {len(df_merged)} rows ({rows_reduced} merged)")
    
    return df_merged


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Power BI-ready derived columns for analytics.
    
    Args:
        df: Base DataFrame
        
    Returns:
        DataFrame with additional columns
    """
    logger.info("Adding derived columns for Power BI...")
    
    # Convert to datetime first for calculations
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    # Date-based features (extract before converting Date to string)
    df['Day_Name'] = df['Date'].dt.day_name()
    df['Day_of_Week'] = df['Date'].dt.dayofweek  # 0=Monday, 6=Sunday
    df['Day_of_Month'] = df['Date'].dt.day
    df['Week_Number'] = df['Date'].dt.isocalendar().week.astype(int)
    df['Month_Number'] = df['Date'].dt.month
    df['Month_Name'] = df['Date'].dt.month_name()
    df['Year'] = df['Date'].dt.year
    df['Is_Weekend'] = (df['Date'].dt.dayofweek >= 5).astype(int)  # 1=Weekend, 0=Weekday
    df['Is_Weekday'] = (df['Date'].dt.dayofweek < 5).astype(int)   # 1=Weekday, 0=Weekend
    
    # Convert Date to YYYY-MM-DD string format (Power BI auto-detects as date)
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    
    # Usage tier classification
    df['Usage_Tier'] = df['Duration_Minutes'].apply(get_usage_tier)
    
    # Duration in hours (for easier reading of large values)
    df['Duration_Hours'] = round(df['Duration_Minutes'] / 60, 2)
    
    # Categorization
    cat_info = df['App'].apply(get_category_info)
    df['Category'] = [x[0] for x in cat_info]
    df['Productivity_Type'] = [x[1] for x in cat_info]
    
    return df


def create_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create daily aggregated summary for time-series analysis.
    
    Args:
        df: Cleaned granular DataFrame
        
    Returns:
        Daily summary DataFrame
    """
    logger.info("Creating daily summary...")
    
    daily = df.groupby('Date').agg({
        'Duration_Minutes': 'sum',
        'App': 'nunique',
        'Category': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'Mixed'
    }).reset_index()
    
    daily.columns = ['Date', 'Total_Minutes', 'Unique_Apps', 'Top_Category']
    daily['Total_Hours'] = round(daily['Total_Minutes'] / 60, 2)
    
    # Add date features
    daily['Day_Name'] = pd.to_datetime(daily['Date']).dt.day_name()
    daily['Is_Weekend'] = pd.to_datetime(daily['Date']).dt.dayofweek >= 5
    daily['Week_Number'] = pd.to_datetime(daily['Date']).dt.isocalendar().week
    
    # Calculate productivity breakdown per day
    prod_daily = df.groupby(['Date', 'Productivity_Type'])['Duration_Minutes'].sum().unstack(fill_value=0)
    prod_daily = prod_daily.reset_index()
    
    daily = daily.merge(prod_daily, on='Date', how='left')
    
    # Sort by date
    daily = daily.sort_values('Date')
    
    return daily


def create_category_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create category-level summary for Power BI category analysis.
    
    Args:
        df: Cleaned granular DataFrame
        
    Returns:
        Category summary DataFrame
    """
    logger.info("Creating category summary...")
    
    cat_summary = df.groupby(['Date', 'Category', 'Productivity_Type']).agg({
        'Duration_Minutes': 'sum',
        'App': 'nunique'
    }).reset_index()
    
    cat_summary.columns = ['Date', 'Category', 'Productivity_Type', 'Total_Minutes', 'App_Count']
    cat_summary['Total_Hours'] = round(cat_summary['Total_Minutes'] / 60, 2)
    
    # Add date features
    cat_summary['Day_Name'] = pd.to_datetime(cat_summary['Date']).dt.day_name()
    cat_summary['Is_Weekend'] = pd.to_datetime(cat_summary['Date']).dt.dayofweek >= 5
    
    return cat_summary.sort_values(['Date', 'Total_Minutes'], ascending=[True, False])


def print_data_quality_report(df: pd.DataFrame, uncategorized_apps: List[str]):
    """
    Print comprehensive data quality report.
    
    Args:
        df: Final cleaned DataFrame
        uncategorized_apps: List of apps that fell into 'Other' category
    """
    print("\n" + "="*70)
    print(" DATA QUALITY REPORT")
    print("="*70)
    
    print(f"\n📊 DATASET OVERVIEW")
    print(f"   Total Records: {len(df):,}")
    print(f"   Unique Apps: {df['App'].nunique()}")
    print(f"   Unique Dates: {df['Date'].nunique()}")
    print(f"   Date Range: {df['Date'].min().strftime('%Y-%m-%d')} to {df['Date'].max().strftime('%Y-%m-%d')}")
    
    print(f"\n⏱️  USAGE STATISTICS")
    total_mins = df['Duration_Minutes'].sum()
    total_hours = total_mins / 60
    print(f"   Total Screen Time: {total_hours:,.1f} hours ({total_mins:,.0f} minutes)")
    print(f"   Daily Average: {total_hours / df['Date'].nunique():.1f} hours/day")
    print(f"   Max Single Session: {df['Duration_Minutes'].max():.1f} minutes")
    
    print(f"\n📂 CATEGORY BREAKDOWN")
    cat_totals = df.groupby('Category')['Duration_Minutes'].sum().sort_values(ascending=False)
    for cat, mins in cat_totals.head(10).items():
        pct = (mins / total_mins) * 100
        print(f"   {cat:20} {mins/60:6.1f}h ({pct:5.1f}%)")
    
    print(f"\n🎯 PRODUCTIVITY BREAKDOWN")
    prod_totals = df.groupby('Productivity_Type')['Duration_Minutes'].sum().sort_values(ascending=False)
    for prod, mins in prod_totals.items():
        pct = (mins / total_mins) * 100
        print(f"   {prod:20} {mins/60:6.1f}h ({pct:5.1f}%)")
    
    print(f"\n📈 USAGE TIER DISTRIBUTION")
    tier_counts = df['Usage_Tier'].value_counts()
    for tier in ['Low', 'Medium', 'High', 'Very High']:
        if tier in tier_counts.index:
            count = tier_counts[tier]
            pct = (count / len(df)) * 100
            print(f"   {tier:15} {count:5,} records ({pct:5.1f}%)")
    
    if uncategorized_apps:
        print(f"\n⚠️  UNCATEGORIZED APPS ({len(uncategorized_apps)} apps)")
        print("   Consider adding these to APP_CATEGORIES:")
        for app in sorted(set(uncategorized_apps))[:15]:
            print(f"   - {app}")
        if len(set(uncategorized_apps)) > 15:
            print(f"   ... and {len(set(uncategorized_apps)) - 15} more")
    
    print("\n" + "="*70)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main ETL pipeline execution."""
    
    print("\n" + "="*70)
    print(" SCREEN TIME DATA ETL PIPELINE")
    print(" BIT34503 Data Science Project")
    print("="*70 + "\n")
    
    start_time = datetime.now()
    logger.info("Starting ETL pipeline...")
    
    # --- STEP 1: EXTRACT ---
    data_frames = []
    
    for file_config in INPUT_FILES:
        df = process_single_file(
            filename=file_config["filename"],
            month_filter=file_config["month_filter"],
            interaction_mode=file_config["interaction_mode"]
        )
        if df is not None:
            data_frames.append(df)
    
    if not data_frames:
        logger.error("No data extracted! Check input files.")
        return
    
    # --- STEP 2: TRANSFORM ---
    logger.info("Combining datasets...")
    combined_df = pd.concat(data_frames, ignore_index=True)
    logger.info(f"  -> Combined: {len(combined_df)} total rows")
    
    # Parse time strings to minutes
    logger.info("Parsing time strings...")
    combined_df['Duration_Minutes'] = combined_df['Usage_Raw'].apply(parse_time_to_minutes)
    
    # Remove zero-usage rows
    initial_count = len(combined_df)
    combined_df = combined_df[combined_df['Duration_Minutes'] > 0]
    removed = initial_count - len(combined_df)
    logger.info(f"  -> Removed {removed} zero-usage rows ({len(combined_df)} remaining)")
    
    # Merge duplicate entries
    combined_df = merge_duplicates(combined_df)
    
    # Add derived columns
    combined_df = add_derived_columns(combined_df)
    
    # Track uncategorized apps
    uncategorized = combined_df[combined_df['Category'] == 'Other']['App'].unique().tolist()
    
    # Sort final dataset
    combined_df = combined_df.sort_values(
        by=['Date', 'Duration_Minutes'],
        ascending=[True, False]
    )
    
    # Reorder columns for Power BI
    column_order = [
        'Date', 'App', 'Category', 'Productivity_Type',
        'Duration_Minutes', 'Duration_Hours', 'Usage_Tier',
        'Day_Name', 'Day_of_Week', 'Day_of_Month', 'Week_Number',
        'Month_Name', 'Month_Year', 'Is_Weekend', 'Is_Weekday',
        'Interaction_Mode', 'Usage_Raw'
    ]
    
    # Add Device column if present
    if 'Device' in combined_df.columns:
        column_order.insert(2, 'Device')
    
    # Keep only existing columns in order
    final_columns = [c for c in column_order if c in combined_df.columns]
    final_df = combined_df[final_columns]
    
    # --- STEP 3: LOAD ---
    logger.info("Saving output files...")
    
    # Main granular dataset
    final_df.to_csv(OUTPUT_MAIN, index=False)
    logger.info(f"  -> Saved: {OUTPUT_MAIN} ({len(final_df)} rows)")
    
    # Daily summary
    daily_df = create_daily_summary(final_df)
    daily_df.to_csv(OUTPUT_DAILY, index=False)
    logger.info(f"  -> Saved: {OUTPUT_DAILY} ({len(daily_df)} rows)")
    
    # Category summary
    category_df = create_category_summary(final_df)
    category_df.to_csv(OUTPUT_CATEGORY, index=False)
    logger.info(f"  -> Saved: {OUTPUT_CATEGORY} ({len(category_df)} rows)")
    
    # --- REPORTING ---
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"ETL pipeline completed in {elapsed:.2f} seconds")
    
    # Print data quality report
    print_data_quality_report(final_df, uncategorized)
    
    print(f"\n✅ SUCCESS! Output files ready for Power BI:")
    print(f"   📁 {OUTPUT_MAIN}")
    print(f"   📁 {OUTPUT_DAILY}")
    print(f"   📁 {OUTPUT_CATEGORY}")
    print()


if __name__ == "__main__":
    main()