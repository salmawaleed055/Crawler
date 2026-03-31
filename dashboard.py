#!/usr/bin/env python3
"""
NAME: Salma Elmarakby
ID: 900232658

DATA.GOV DASHBOARD - MILESTONE 2
Interactive Streamlit Dashboard for analyzing 2,000 datasets from data.gov API
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime
import json

# Page configuration
st.set_page_config(
    page_title="Data.gov Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main {
        padding-top: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)

# Load data
@st.cache_data
def load_data():
    """Load all datasets from CSV files"""
    try:
        df_raw = pd.read_csv('output/raw_data/crawled_datasets_api.csv')
        return df_raw
    except FileNotFoundError:
        st.error("❌ CSV file not found. Please run `python complete_pipeline.py` first.")
        return None

# Load datasets
df = load_data()

if df is None:
    st.stop()

# ============================================================
# HEADER
# ============================================================
st.title("📊 Data.gov API Dashboard")
st.markdown("**Exploring 2,000+ Datasets from the data.gov Catalog**")
st.divider()

# ============================================================
# KEY METRICS
# ============================================================
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Datasets", f"{len(df):,}")

with col2:
    unique_orgs = df['organization'].nunique()
    st.metric("Organizations", f"{unique_orgs}")

with col3:
    has_email = df[df['maintainer_email'] != 'N/A'].shape[0]
    st.metric("With Email", f"{has_email:,}")

with col4:
    has_desc = df[df['description'] != 'N/A'].shape[0]
    st.metric("With Description", f"{has_desc:,}")

with col5:
    public_count = df[df['access_level'].str.lower() == 'public'].shape[0]
    st.metric("Public Access", f"{public_count:,}")

st.divider()

# ============================================================
# TABS FOR DIFFERENT SECTIONS
# ============================================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 Overview",
    "🏢 Organizations",
    "📜 Licenses",
    "🏷️ Formats & Topics",
    "🔍 Search & Filter",
    "📊 Data Quality"
])

# ============================================================
# TAB 1: OVERVIEW
# ============================================================
with tab1:
    st.subheader("Dataset Overview")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Access Level Distribution
        access_counts = df['access_level'].value_counts()
        fig_access = px.pie(
            values=access_counts.values,
            names=access_counts.index,
            title="Dataset Access Level",
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        st.plotly_chart(fig_access, use_container_width=True)
    
    with col2:
        # License Type Distribution (Top 15)
        license_counts = df['license'].value_counts().head(15)
        fig_license = px.bar(
            x=license_counts.values,
            y=license_counts.index,
            orientation='h',
            title="Top 15 License Types",
            labels={'x': 'Count', 'y': 'License'},
            color=license_counts.values,
            color_continuous_scale='viridis'
        )
        fig_license.update_layout(height=500, showlegend=False)
        st.plotly_chart(fig_license, use_container_width=True)
    
    # Dataset Creation Timeline
    st.subheader("Dataset Creation Timeline")
    df['creation_date_parsed'] = pd.to_datetime(df['creation_date'], errors='coerce')
    df_timeline = df[df['creation_date_parsed'].notna()].copy()
    df_timeline['year'] = df_timeline['creation_date_parsed'].dt.year
    timeline_counts = df_timeline['year'].value_counts().sort_index()
    
    fig_timeline = px.line(
        x=timeline_counts.index,
        y=timeline_counts.values,
        markers=True,
        title="Datasets Created by Year",
        labels={'x': 'Year', 'y': 'Number of Datasets'},
        line_shape='linear'
    )
    fig_timeline.update_traces(marker=dict(size=8))
    st.plotly_chart(fig_timeline, use_container_width=True)

# ============================================================
# TAB 2: ORGANIZATIONS
# ============================================================
with tab2:
    st.subheader("Organization Analysis")
    
    # Top organizations
    top_orgs = df['organization'].value_counts().head(20)
    
    col1, col2 = st.columns([1.5, 1])
    
    with col1:
        fig_orgs = px.bar(
            x=top_orgs.values,
            y=top_orgs.index,
            orientation='h',
            title="Top 20 Organizations by Dataset Count",
            labels={'x': 'Number of Datasets', 'y': 'Organization'},
            color=top_orgs.values,
            color_continuous_scale='teal'
        )
        fig_orgs.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig_orgs, use_container_width=True)
    
    with col2:
        st.metric("Total Organizations", df['organization'].nunique())
        st.metric("Top Organization", f"{top_orgs.index[0]} ({top_orgs.values[0]} datasets)")
        st.metric("Datasets without Org", df[df['organization'] == 'N/A'].shape[0])

# ============================================================
# TAB 3: LICENSES
# ============================================================
with tab3:
    st.subheader("License Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # License type breakdown
        license_counts = df['license'].value_counts()
        fig_license_pie = px.pie(
            values=license_counts.values,
            names=license_counts.index,
            title=f"All License Types ({len(license_counts)} unique)",
            height=600
        )
        st.plotly_chart(fig_license_pie, use_container_width=True)
    
    with col2:
        st.metric("Unique Licenses", df['license'].nunique())
        
        # Top licenses
        st.subheader("Top 10 Licenses")
        top_licenses = df['license'].value_counts().head(10)
        for idx, (lic, count) in enumerate(top_licenses.items(), 1):
            pct = (count / len(df)) * 100
            st.write(f"**{idx}. {lic}**")
            st.progress(count / len(df), text=f"{count} datasets ({pct:.1f}%)")

# ============================================================
# TAB 4: FORMATS & TOPICS
# ============================================================
with tab4:
    st.subheader("File Formats & Topics Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Extract individual formats
        all_formats = []
        for formats_str in df['formats'].dropna():
            if formats_str != 'N/A':
                formats = [f.strip() for f in str(formats_str).split(',')]
                all_formats.extend(formats)
        
        format_counts = pd.Series(all_formats).value_counts().head(15)
        
        fig_formats = px.bar(
            x=format_counts.index,
            y=format_counts.values,
            title="Top 15 File Formats",
            labels={'x': 'Format', 'y': 'Count'},
            color=format_counts.values,
            color_continuous_scale='plasma'
        )
        fig_formats.update_xaxes(tickangle=-45)
        st.plotly_chart(fig_formats, use_container_width=True)
    
    with col2:
        # Extract topics/tags
        all_topics = []
        for topic_str in df['topic'].dropna():
            if topic_str != 'N/A':
                topics = [t.strip() for t in str(topic_str).split(',')]
                all_topics.extend(topics)
        
        topic_counts = pd.Series(all_topics).value_counts().head(15)
        
        fig_topics = px.bar(
            x=topic_counts.index,
            y=topic_counts.values,
            title="Top 15 Topics/Tags",
            labels={'x': 'Topic', 'y': 'Count'},
            color=topic_counts.values,
            color_continuous_scale='blues'
        )
        fig_topics.update_xaxes(tickangle=-45)
        st.plotly_chart(fig_topics, use_container_width=True)

# ============================================================
# TAB 5: SEARCH & FILTER
# ============================================================
with tab5:
    st.subheader("Search & Filter Datasets")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        search_term = st.text_input("🔍 Search Dataset Name", "")
    
    with col2:
        org_options = sorted([x for x in df['organization'].unique() if pd.notna(x)])
        selected_org = st.selectbox(
            "🏢 Filter by Organization",
            ["All"] + org_options
        )
    
    with col3:
        license_options = sorted([x for x in df['license'].unique() if pd.notna(x)])
        selected_license = st.selectbox(
            "📜 Filter by License",
            ["All"] + license_options
        )
    
    # Apply filters
    filtered_df = df.copy()
    
    if search_term:
        filtered_df = filtered_df[
            filtered_df['name'].str.contains(search_term, case=False, na=False)
        ]
    
    if selected_org != "All":
        filtered_df = filtered_df[filtered_df['organization'] == selected_org]
    
    if selected_license != "All":
        filtered_df = filtered_df[filtered_df['license'] == selected_license]
    
    st.metric("Matching Datasets", len(filtered_df))
    
    if len(filtered_df) > 0:
        # Display results
        st.subheader(f"Results ({len(filtered_df)} datasets)")
        
        # Show first 50 results
        display_cols = ['name', 'organization', 'license', 'maintainer_email', 'formats']
        
        for idx, row in filtered_df.head(50).iterrows():
            with st.expander(f"📂 {row['name'][:60]}...", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Organization:** {row['organization']}")
                    st.write(f"**License:** {row['license']}")
                    st.write(f"**Access Level:** {row['access_level']}")
                    st.write(f"**Formats:** {row['formats']}")
                
                with col2:
                    st.write(f"**Maintainer:** {row['maintainer']}")
                    st.write(f"**Email:** {row['maintainer_email']}")
                    st.write(f"**Created:** {row['creation_date']}")
                    st.write(f"**Modified:** {row['metadata_modified']}")
                
                if row['description'] != 'N/A':
                    st.write(f"**Description:** {row['description'][:200]}...")
                
                st.write(f"**URL:** {row['url']}")
    else:
        st.warning("No datasets match your filters.")

# ============================================================
# TAB 6: DATA QUALITY
# ============================================================
with tab6:
    st.subheader("Data Quality Metrics")
    
    # Calculate quality metrics
    metrics = {
        'Total Records': len(df),
        'Complete Names': df['name'].apply(lambda x: x != 'N/A').sum(),
        'Complete URLs': df['url'].apply(lambda x: x != 'N/A').sum(),
        'Complete Organizations': df['organization'].apply(lambda x: x != 'N/A').sum(),
        'Complete Licenses': df['license'].apply(lambda x: x != 'N/A').sum(),
        'Complete Descriptions': df['description'].apply(lambda x: x != 'N/A').sum(),
        'Complete Topics': df['topic'].apply(lambda x: x != 'N/A').sum(),
        'Complete Maintainer Email': df['maintainer_email'].apply(lambda x: x != 'N/A').sum(),
        'Complete Creation Date': df['creation_date'].apply(lambda x: x != 'N/A').sum(),
        'Complete Metadata Modified': df['metadata_modified'].apply(lambda x: x != 'N/A').sum(),
    }
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Completeness Score")
        
        quality_data = []
        for field, count in list(metrics.items())[1:]:
            completeness = (count / metrics['Total Records']) * 100
            quality_data.append({
                'Field': field.replace('Complete ', ''),
                'Completeness %': completeness,
                'Count': count
            })
        
        quality_df = pd.DataFrame(quality_data)
        
        fig_quality = px.bar(
            quality_df,
            x='Field',
            y='Completeness %',
            title="Data Completeness by Field",
            labels={'Completeness %': 'Completeness (%)'},
            color='Completeness %',
            color_continuous_scale='RdYlGn',
            range_color=[0, 100]
        )
        fig_quality.update_xaxes(tickangle=-45)
        st.plotly_chart(fig_quality, use_container_width=True)
    
    with col2:
        st.subheader("Overall Quality Metrics")
        
        avg_completeness = quality_df['Completeness %'].mean()
        st.metric("Average Completeness", f"{avg_completeness:.1f}%")
        
        st.write("**Field-Level Completeness:**")
        for _, row in quality_df.iterrows():
            st.write(f"- **{row['Field']}**: {row['Completeness %']:.1f}% ({int(row['Count'])} / {metrics['Total Records']})")
    
    # Data Issues
    st.subheader("Data Issues Identified")
    
    issues = []
    
    if (df['description'] == 'N/A').sum() > 0:
        issues.append(f"❌ {(df['description'] == 'N/A').sum()} datasets missing descriptions")
    
    if (df['maintainer_email'] == 'N/A').sum() > 0:
        issues.append(f"❌ {(df['maintainer_email'] == 'N/A').sum()} datasets missing maintainer email")
    
    if (df['topic'] == 'N/A').sum() > 0:
        issues.append(f"❌ {(df['topic'] == 'N/A').sum()} datasets missing topics")
    
    if len(issues) == 0:
        st.success("✅ No major data quality issues detected!")
    else:
        for issue in issues:
            st.warning(issue)

# ============================================================
# FOOTER
# ============================================================
st.divider()

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📥 Export Data", use_container_width=True):
        csv = df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"data_gov_datasets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

with col2:
    if st.button("📄 Export Summary Report", use_container_width=True):
        report = f"""
DATA.GOV DASHBOARD SUMMARY REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

OVERVIEW
--------
Total Datasets: {len(df):,}
Total Organizations: {df['organization'].nunique()}
Unique Licenses: {df['license'].nunique()}

TOP STATISTICS
--------------
Most Common License: {df['license'].value_counts().index[0]} ({df['license'].value_counts().values[0]} datasets)
Most Active Organization: {df['organization'].value_counts().index[0]} ({df['organization'].value_counts().values[0]} datasets)
Datasets with Email: {(df['maintainer_email'] != 'N/A').sum():,}
Datasets with Description: {(df['description'] != 'N/A').sum():,}

Access Levels
{df['access_level'].value_counts().to_string()}

"""
        st.download_button(
            label="Download Report",
            data=report,
            file_name=f"data_gov_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            mime="text/plain"
        )

with col3:
    st.info(f"✅ Dashboard generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

st.markdown("---")
st.markdown("""
**Project:** Milestone 2 - Data.gov API Analysis  
**Author:** Salma Elmarakby (ID: 900232658)  
**Data Source:** data.gov CKAN API - 2 requests for 2,000 datasets
""")
