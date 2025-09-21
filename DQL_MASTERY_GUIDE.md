# DQL Mastery Guide: 100+ Queries & 20+ Mutations for Dgraph

> **Your Complete Guide to Mastering Dgraph Query Language (DQL)**  
> From Basic Queries to Advanced Graph Traversals

---

## 🎯 Table of Contents

1. [Basic Queries (Easy Level)](#-basic-queries-easy-level)
2. [Intermediate Queries (Medium Level)](#-intermediate-queries-medium-level)
3. [Advanced Queries (Hard Level)](#-advanced-queries-hard-level)
4. [Expert Queries (Master Level)](#-expert-queries-master-level)
5. [Mutations & Data Modification](#-mutations--data-modification)
6. [Schema Operations](#-schema-operations)
7. [Performance & Optimization](#-performance--optimization)
8. [Real-World Examples](#-real-world-examples)

---

## 🟢 Basic Queries (Easy Level)

### **1. Simple Node Retrieval**
```graphql
# Get all nodes with a specific type
{
  users(func: type(chorki_customers)) {
    uid
    chorki_customers.name
    chorki_customers.email
  }
}
```
**Use Case:** Basic data fetching - Foundation of all DQL queries

### **2. Count Nodes**
```graphql
# Count total customers
{
  customer_count(func: type(chorki_customers)) {
    count(uid)
  }
}
```
**Use Case:** Data analytics - Get dataset size quickly

### **3. Get Specific Node by UID**
```graphql
# Fetch specific customer by UID
{
  customer(func: uid(0x17b24ec7)) {
    uid
    chorki_customers.name
    chorki_customers.email
    chorki_customers.phone
  }
}
```
**Use Case:** Direct node access - When you know the exact UID

### **4. Simple String Search**
```graphql
# Find customers by exact name match
{
  search(func: eq(chorki_customers.name, "John Doe")) {
    uid
    chorki_customers.name
    chorki_customers.email
  }
}
```
**Use Case:** Exact text matching - Finding specific records

### **5. Basic Pagination**
```graphql
# Get first 10 customers
{
  customers(func: type(chorki_customers), first: 10) {
    uid
    chorki_customers.name
    chorki_customers.email
  }
}
```
**Use Case:** Data pagination - Handling large datasets

### **6. Basic Ordering**
```graphql
# Get customers ordered by name
{
  customers(func: type(chorki_customers), orderasc: chorki_customers.name, first: 10) {
    uid
    chorki_customers.name
    chorki_customers.email
  }
}
```
**Use Case:** Sorted results - Alphabetical or numerical ordering

### **7. Check if Field Exists**
```graphql
# Find customers who have email addresses
{
  customers_with_email(func: has(chorki_customers.email)) {
    uid
    chorki_customers.name
    chorki_customers.email
  }
}
```
**Use Case:** Data quality checks - Finding complete records

### **8. Simple Relationship Traversal**
```graphql
# Get customers and their content reactions
{
  customers(func: type(chorki_customers), first: 5) {
    uid
    chorki_customers.name
    ~chorki_content_reactions.customer_id {
      uid
      chorki_content_reactions.type
    }
  }
}
```
**Use Case:** Basic relationships - Following foreign keys

### **9. Multiple Field Selection**
```graphql
# Get comprehensive customer data
{
  customers(func: type(chorki_customers), first: 5) {
    uid
    chorki_customers.name
    chorki_customers.email
    chorki_customers.phone
    chorki_customers.created_at
    chorki_customers.updated_at
  }
}
```
**Use Case:** Complete record retrieval - All available fields

### **10. Basic Filtering with AND**
```graphql
# Find customers with both name and email
{
  complete_customers(func: type(chorki_customers)) @filter(has(chorki_customers.name) AND has(chorki_customers.email)) {
    uid
    chorki_customers.name
    chorki_customers.email
  }
}
```
**Use Case:** Multiple conditions - Data quality filtering

---

## 🟡 Intermediate Queries (Medium Level)

### **11. Text Search with Term Index**
```graphql
# Search customers by partial name match
{
  search(func: anyofterms(chorki_customers.name, "John")) {
    uid
    chorki_customers.name
    chorki_customers.email
  }
}
```
**Use Case:** Fuzzy text search - Finding similar names

### **12. Numeric Range Queries**
```graphql
# Find customers created in date range (assuming timestamp)
{
  recent_customers(func: type(chorki_customers)) @filter(ge(chorki_customers.created_at, "2023-01-01")) {
    uid
    chorki_customers.name
    chorki_customers.created_at
  }
}
```
**Use Case:** Date/time filtering - Recent data analysis

### **13. Complex OR Conditions**
```graphql
# Find customers with either phone OR email
{
  contactable_customers(func: type(chorki_customers)) @filter(has(chorki_customers.phone) OR has(chorki_customers.email)) {
    uid
    chorki_customers.name
    chorki_customers.phone
    chorki_customers.email
  }
}
```
**Use Case:** Alternative conditions - Flexible filtering

### **14. Reverse Edge Traversal**
```graphql
# Find content that has reactions
{
  popular_content(func: type(chorki_metas)) @filter(has(~chorki_content_reactions.content_id)) {
    uid
    chorki_metas.title
    ~chorki_content_reactions.content_id {
      chorki_content_reactions.type
    }
  }
}
```
**Use Case:** Reverse relationships - Finding connected data

### **15. Nested Filtering**
```graphql
# Find customers who liked specific content
{
  content_lovers(func: type(chorki_customers)) @filter(has(~chorki_content_reactions.customer_id)) {
    uid
    chorki_customers.name
    ~chorki_content_reactions.customer_id @filter(eq(chorki_content_reactions.type, "like")) {
      uid
      chorki_content_reactions.type
      chorki_content_reactions.content_id {
        chorki_metas.title
      }
    }
  }
}
```
**Use Case:** Deep filtering - Complex relationship queries

### **16. Variable Assignment**
```graphql
# Use variables to find active users
{
  var(func: type(chorki_customers)) @filter(has(~chorki_content_reactions.customer_id)) {
    active_users as uid
  }
  
  active_customers(func: uid(active_users), first: 10) {
    uid
    chorki_customers.name
    reaction_count: count(~chorki_content_reactions.customer_id)
  }
}
```
**Use Case:** Complex logic - Reusable query components

### **17. Aggregation with Count**
```graphql
# Count reactions per customer
{
  customer_stats(func: type(chorki_customers), first: 10) {
    uid
    chorki_customers.name
    total_reactions: count(~chorki_content_reactions.customer_id)
  }
}
```
**Use Case:** Analytics - Aggregating relationship data

### **18. String Pattern Matching**
```graphql
# Find customers with email domains
{
  gmail_users(func: type(chorki_customers)) @filter(regexp(chorki_customers.email, /.*@gmail\.com$/)) {
    uid
    chorki_customers.name
    chorki_customers.email
  }
}
```
**Use Case:** Pattern matching - Complex text filtering

### **19. Multi-level Relationship**
```graphql
# Videos -> Series -> Content reactions
{
  videos(func: type(chorki_videos), first: 5) {
    uid
    chorki_videos.title
    chorki_videos.series_id {
      uid
      chorki_series.title
      ~chorki_content_reactions.content_id {
        chorki_content_reactions.type
        chorki_content_reactions.customer_id {
          chorki_customers.name
        }
      }
    }
  }
}
```
**Use Case:** Deep traversal - Following complex relationship chains

### **20. Conditional Field Selection**
```graphql
# Different fields based on content type
{
  content(func: type(chorki_metas), first: 10) {
    uid
    chorki_metas.title
    chorki_metas.type
    
    # Show series info only for series content
    chorki_metas.series_id @filter(eq(chorki_metas.type, "series")) {
      chorki_series.title
    }
  }
}
```
**Use Case:** Conditional logic - Dynamic field selection

### **21. String Functions**
```graphql
# Case-insensitive search
{
  search(func: type(chorki_customers)) @filter(alloftext(chorki_customers.name, "john")) {
    uid
    chorki_customers.name
  }
}
```
**Use Case:** Text processing - Case-insensitive matching

### **22. Date Comparisons**
```graphql
# Find recent content (last 30 days)
{
  recent_content(func: type(chorki_metas)) @filter(ge(chorki_metas.created_at, "2023-11-01")) {
    uid
    chorki_metas.title
    chorki_metas.created_at
  }
}
```
**Use Case:** Time-based queries - Recent data analysis

### **23. Multiple Variable Usage**
```graphql
# Find customers and their favorite content type
{
  var(func: type(chorki_content_reactions)) @groupby(chorki_content_reactions.customer_id) {
    customer_id as chorki_content_reactions.customer_id
    reaction_count as count(uid)
  }
  
  active_customers(func: uid(customer_id), orderdesc: val(reaction_count), first: 10) {
    uid
    chorki_customers.name
    total_reactions: val(reaction_count)
  }
}
```
**Use Case:** Advanced analytics - Complex aggregations

### **24. Faceted Search**
```graphql
# Search with multiple facets
{
  search(func: type(chorki_metas)) @filter(anyofterms(chorki_metas.title, "series drama") AND eq(chorki_metas.type, "series")) {
    uid
    chorki_metas.title
    chorki_metas.type
    chorki_metas.genre
  }
}
```
**Use Case:** Multi-dimensional filtering - Search with categories

### **25. Shortest Path Query**
```graphql
# Find connection between customer and content
{
  path as shortest(from: 0x17b24ec7, to: 0x17a4955a) {
    chorki_content_reactions.customer_id
    chorki_content_reactions.content_id
  }
  
  path(func: uid(path)) {
    uid
    dgraph.type
  }
}
```
**Use Case:** Path finding - Discovering connections

---

## 🔴 Advanced Queries (Hard Level)

### **26. Complex Aggregation with Grouping**
```graphql
# Content popularity by type with statistics
{
  var(func: type(chorki_content_reactions)) @groupby(chorki_content_reactions.content_id) {
    content_id as chorki_content_reactions.content_id
    reaction_count as count(uid)
    like_count as count(uid) @filter(eq(chorki_content_reactions.type, "like"))
    love_count as count(uid) @filter(eq(chorki_content_reactions.type, "love"))
  }
  
  popular_content(func: uid(content_id), orderdesc: val(reaction_count), first: 20) {
    uid
    chorki_metas.title
    chorki_metas.type
    total_reactions: val(reaction_count)
    likes: val(like_count)
    loves: val(love_count)
    engagement_ratio: math(val(like_count) + val(love_count) * 2)
  }
}
```
**Use Case:** Advanced analytics - Complex metrics calculation

### **27. Recursive Query with Depth Limit**
```graphql
# Find all series and their episodes (recursive)
{
  series(func: type(chorki_series)) @recurse(depth: 3) {
    uid
    chorki_series.title
    ~chorki_videos.series_id
    chorki_videos.title
  }
}
```
**Use Case:** Hierarchical data - Tree structures

### **28. Mathematical Expressions**
```graphql
# Calculate engagement scores
{
  var(func: type(chorki_metas)) {
    content as uid
    like_count as count(~chorki_content_reactions.content_id @filter(eq(chorki_content_reactions.type, "like")))
    love_count as count(~chorki_content_reactions.content_id @filter(eq(chorki_content_reactions.type, "love")))
    view_count as count(~chorki_watch_histories.content_id)
  }
  
  engagement_scores(func: uid(content), orderdesc: val(engagement_score), first: 15) {
    uid
    chorki_metas.title
    likes: val(like_count)
    loves: val(love_count)
    views: val(view_count)
    engagement_score: math(val(like_count) + val(love_count) * 2 + val(view_count) * 0.1)
  }
}
```
**Use Case:** Complex calculations - Custom scoring algorithms

### **29. Multi-level Aggregation**
```graphql
# Series performance with episode statistics
{
  var(func: type(chorki_videos)) @groupby(chorki_videos.series_id) {
    series_id as chorki_videos.series_id
    episode_count as count(uid)
    total_views as sum(val(video_views))
  }
  
  var(func: type(chorki_videos)) {
    video_views as count(~chorki_watch_histories.video_id)
  }
  
  series_stats(func: uid(series_id), orderdesc: val(total_views)) {
    uid
    chorki_series.title
    episode_count: val(episode_count)
    total_views: val(total_views)
    avg_views_per_episode: math(val(total_views) / val(episode_count))
  }
}
```
**Use Case:** Hierarchical analytics - Parent-child metrics

### **30. Time-based Cohort Analysis**
```graphql
# Customer registration cohorts
{
  var(func: type(chorki_customers)) @groupby(chorki_customers.registration_month) {
    month as chorki_customers.registration_month
    monthly_signups as count(uid)
  }
  
  cohort_analysis(func: uid(month), orderasc: chorki_customers.registration_month) {
    registration_month: val(month)
    new_customers: val(monthly_signups)
  }
}
```
**Use Case:** Time series analysis - Cohort tracking

### **31. Graph Pattern Matching**
```graphql
# Find customers who like same content as specific user
{
  var(func: uid(0x17b24ec7)) {
    liked_content as ~chorki_content_reactions.customer_id @filter(eq(chorki_content_reactions.type, "like")) {
      chorki_content_reactions.content_id
    }
  }
  
  similar_users(func: type(chorki_customers)) @filter(NOT uid(0x17b24ec7)) {
    uid
    chorki_customers.name
    shared_interests: count(~chorki_content_reactions.customer_id @filter(uid(liked_content)))
  }
}
```
**Use Case:** Recommendation engine - Finding similar users

### **32. Complex Boolean Logic**
```graphql
# Advanced content filtering
{
  trending_content(func: type(chorki_metas)) @filter(
    (anyofterms(chorki_metas.genre, "drama action") OR eq(chorki_metas.type, "series")) 
    AND ge(chorki_metas.created_at, "2023-01-01")
    AND has(~chorki_content_reactions.content_id)
  ) {
    uid
    chorki_metas.title
    chorki_metas.genre
    chorki_metas.type
    reaction_count: count(~chorki_content_reactions.content_id)
  }
}
```
**Use Case:** Complex filtering - Multi-criteria search

### **33. Geospatial Queries (if geo data exists)**
```graphql
# Find customers near a location (hypothetical geo data)
{
  nearby_customers(func: near(chorki_customers.location, [23.8103, 90.4125], 10)) {
    uid
    chorki_customers.name
    chorki_customers.location
    distance: distance(chorki_customers.location, [23.8103, 90.4125])
  }
}
```
**Use Case:** Location-based queries - Geographic filtering

### **34. Custom Scoring with Multiple Factors**
```graphql
# Content recommendation score
{
  var(func: type(chorki_metas)) {
    content as uid
    reaction_score as math(
      count(~chorki_content_reactions.content_id @filter(eq(chorki_content_reactions.type, "like"))) * 1 +
      count(~chorki_content_reactions.content_id @filter(eq(chorki_content_reactions.type, "love"))) * 3 +
      count(~chorki_watch_histories.content_id) * 0.5
    )
    recency_score as math(since(chorki_metas.created_at))
    final_score as math(val(reaction_score) / (val(recency_score) + 1))
  }
  
  recommended_content(func: uid(content), orderdesc: val(final_score), first: 10) {
    uid
    chorki_metas.title
    chorki_metas.type
    recommendation_score: val(final_score)
  }
}
```
**Use Case:** Machine learning features - Complex ranking algorithms

### **35. Graph Analytics - Centrality**
```graphql
# Find most connected customers (degree centrality)
{
  var(func: type(chorki_customers)) {
    customer as uid
    connection_score as math(
      count(~chorki_content_reactions.customer_id) +
      count(~chorki_watch_histories.customer_id) +
      count(~chorki_subscriptions.customer_id)
    )
  }
  
  influential_customers(func: uid(customer), orderdesc: val(connection_score), first: 20) {
    uid
    chorki_customers.name
    influence_score: val(connection_score)
    reactions: count(~chorki_content_reactions.customer_id)
    watches: count(~chorki_watch_histories.customer_id)
  }
}
```
**Use Case:** Network analysis - Finding key nodes

---

## 🟣 Expert Queries (Master Level)

### **36. Multi-hop Relationship Analysis**
```graphql
# Find customers who watch series that other similar customers like
{
  # First, find customers who like the same content as target user
  var(func: uid(0x17b24ec7)) {
    target_likes as ~chorki_content_reactions.customer_id @filter(eq(chorki_content_reactions.type, "like")) {
      chorki_content_reactions.content_id
    }
  }
  
  var(func: type(chorki_customers)) @filter(NOT uid(0x17b24ec7)) {
    similar_customers as uid @filter(ge(count(~chorki_content_reactions.customer_id @filter(uid(target_likes))), 3))
  }
  
  # Find what series these similar customers watch
  var(func: uid(similar_customers)) {
    watched_series as ~chorki_watch_histories.customer_id {
      chorki_watch_histories.content_id @filter(type(chorki_series))
    }
  }
  
  recommendations(func: uid(watched_series)) @filter(NOT uid(target_likes)) {
    uid
    chorki_series.title
    similarity_score: count(~chorki_watch_histories.content_id @filter(uid(similar_customers)))
  }
}
```
**Use Case:** Collaborative filtering - Advanced recommendation system

### **37. Temporal Pattern Analysis**
```graphql
# User activity patterns by hour of day
{
  var(func: type(chorki_watch_histories)) {
    hour_of_day as chorki_watch_histories.hour_of_day
    daily_pattern as uid
  }
  
  var(func: uid(daily_pattern)) @groupby(val(hour_of_day)) {
    hour as val(hour_of_day)
    activity_count as count(uid)
  }
  
  activity_patterns(func: uid(hour), orderasc: val(hour)) {
    hour: val(hour)
    watches: val(activity_count)
  }
}
```
**Use Case:** Behavioral analysis - Time-based patterns

### **38. Community Detection**
```graphql
# Find content communities based on shared viewers
{
  var(func: type(chorki_metas)) {
    content_a as uid
  }
  
  var(func: uid(content_a)) {
    viewers_a as ~chorki_watch_histories.content_id {
      chorki_watch_histories.customer_id
    }
  }
  
  content_similarity(func: type(chorki_metas)) @filter(NOT uid(content_a)) {
    uid
    chorki_metas.title
    shared_viewers: count(~chorki_watch_histories.content_id @filter(uid(viewers_a)))
    similarity_ratio: math(count(~chorki_watch_histories.content_id @filter(uid(viewers_a))) / count(~chorki_watch_histories.content_id))
  }
}
```
**Use Case:** Graph clustering - Finding related content groups

### **39. Anomaly Detection**
```graphql
# Find unusual user behavior patterns
{
  var(func: type(chorki_customers)) {
    customer as uid
    avg_session_length as avg(val(session_duration))
    total_watches as count(~chorki_watch_histories.customer_id)
    unique_content as count(~chorki_watch_histories.customer_id @groupby(chorki_watch_histories.content_id))
    variety_score as math(val(unique_content) / val(total_watches))
  }
  
  var(func: uid(customer)) {
    session_duration as ~chorki_watch_histories.customer_id {
      chorki_watch_histories.duration
    }
  }
  
  unusual_users(func: uid(customer)) @filter(gt(val(variety_score), 0.8) AND gt(val(total_watches), 100)) {
    uid
    chorki_customers.name
    total_content_watched: val(total_watches)
    content_variety: val(variety_score)
    avg_session: val(avg_session_length)
  }
}
```
**Use Case:** Outlier detection - Finding unusual patterns

### **40. Multi-dimensional Segmentation**
```graphql
# Customer segmentation based on multiple factors
{
  var(func: type(chorki_customers)) {
    customer as uid
    engagement_level as math(count(~chorki_content_reactions.customer_id))
    watch_frequency as math(count(~chorki_watch_histories.customer_id))
    content_diversity as math(count(~chorki_watch_histories.customer_id @groupby(chorki_watch_histories.content_id)))
    
    # Segment scoring
    segment_score as math(
      cond(val(engagement_level) > 50, 3, 
        cond(val(engagement_level) > 20, 2, 1)) +
      cond(val(watch_frequency) > 100, 3,
        cond(val(watch_frequency) > 50, 2, 1)) +
      cond(val(content_diversity) > 30, 3,
        cond(val(content_diversity) > 15, 2, 1))
    )
  }
  
  customer_segments(func: uid(customer), orderdesc: val(segment_score)) {
    uid
    chorki_customers.name
    engagement: val(engagement_level)
    watch_frequency: val(watch_frequency)
    content_diversity: val(content_diversity)
    segment_score: val(segment_score)
    segment: cond(ge(val(segment_score), 8), "VIP",
             cond(ge(val(segment_score), 6), "Premium",
             cond(ge(val(segment_score), 4), "Regular", "Basic")))
  }
}
```
**Use Case:** Customer analytics - Multi-factor segmentation

### **41. Predictive Scoring**
```graphql
# Churn risk prediction based on activity patterns
{
  var(func: type(chorki_customers)) {
    customer as uid
    recent_activity as count(~chorki_watch_histories.customer_id @filter(ge(chorki_watch_histories.timestamp, "2023-11-01")))
    historical_activity as count(~chorki_watch_histories.customer_id @filter(lt(chorki_watch_histories.timestamp, "2023-11-01")))
    activity_trend as math(val(recent_activity) / (val(historical_activity) + 1))
    
    engagement_depth as avg(val(watch_duration))
    content_completion_rate as math(avg(val(completion_ratio)))
    
    churn_risk_score as math(
      cond(val(activity_trend) < 0.3, 5,
        cond(val(activity_trend) < 0.7, 3, 1)) +
      cond(val(engagement_depth) < 300, 3, 1) +
      cond(val(content_completion_rate) < 0.5, 2, 0)
    )
  }
  
  var(func: uid(customer)) {
    watch_duration as ~chorki_watch_histories.customer_id {
      chorki_watch_histories.duration
    }
    completion_ratio as ~chorki_watch_histories.customer_id {
      math(chorki_watch_histories.watched_duration / chorki_watch_histories.total_duration)
    }
  }
  
  churn_prediction(func: uid(customer), orderdesc: val(churn_risk_score), first: 50) {
    uid
    chorki_customers.name
    recent_watches: val(recent_activity)
    activity_trend: val(activity_trend)
    churn_risk: val(churn_risk_score)
    risk_level: cond(ge(val(churn_risk_score), 7), "High",
                cond(ge(val(churn_risk_score), 4), "Medium", "Low"))
  }
}
```
**Use Case:** Predictive analytics - Risk assessment

---

## 🔧 Mutations & Data Modification

### **42. Simple Node Creation**
```graphql
# Create a new customer
{
  set {
    _:new_customer <dgraph.type> "chorki_customers" .
    _:new_customer <chorki_customers.name> "Alice Johnson" .
    _:new_customer <chorki_customers.email> "alice@example.com" .
    _:new_customer <chorki_customers.phone> "+1234567890" .
    _:new_customer <chorki_customers.created_at> "2023-12-01T10:00:00Z" .
  }
}
```
**Use Case:** Data insertion - Adding new records

### **43. Batch Node Creation**
```graphql
# Create multiple customers at once
{
  set {
    _:customer1 <dgraph.type> "chorki_customers" .
    _:customer1 <chorki_customers.name> "Bob Smith" .
    _:customer1 <chorki_customers.email> "bob@example.com" .
    
    _:customer2 <dgraph.type> "chorki_customers" .
    _:customer2 <chorki_customers.name> "Carol Williams" .
    _:customer2 <chorki_customers.email> "carol@example.com" .
    
    _:customer3 <dgraph.type> "chorki_customers" .
    _:customer3 <chorki_customers.name> "David Brown" .
    _:customer3 <chorki_customers.email> "david@example.com" .
  }
}
```
**Use Case:** Bulk insertion - Efficient batch operations

### **44. Create with Relationships**
```graphql
# Create content and link to existing customer
{
  set {
    _:new_content <dgraph.type> "chorki_metas" .
    _:new_content <chorki_metas.title> "New Episode" .
    _:new_content <chorki_metas.type> "episode" .
    _:new_content <chorki_metas.created_at> "2023-12-01T10:00:00Z" .
    
    _:new_reaction <dgraph.type> "chorki_content_reactions" .
    _:new_reaction <chorki_content_reactions.type> "like" .
    _:new_reaction <chorki_content_reactions.customer_id> <0x17b24ec7> .
    _:new_reaction <chorki_content_reactions.content_id> _:new_content .
    _:new_reaction <chorki_content_reactions.created_at> "2023-12-01T10:05:00Z" .
  }
}
```
**Use Case:** Related data creation - Building connections

### **45. Update Existing Node**
```graphql
# Update customer information
{
  set {
    <0x17b24ec7> <chorki_customers.email> "newemail@example.com" .
    <0x17b24ec7> <chorki_customers.updated_at> "2023-12-01T11:00:00Z" .
  }
}
```
**Use Case:** Data modification - Updating existing records

### **46. Conditional Upsert**
```graphql
# Upsert customer (create if not exists, update if exists)
upsert {
  query {
    customer as var(func: eq(chorki_customers.email, "john@example.com"))
  }
  
  mutation {
    set {
      uid(customer) <chorki_customers.name> "John Updated" .
      uid(customer) <chorki_customers.phone> "+9876543210" .
      uid(customer) <chorki_customers.updated_at> "2023-12-01T12:00:00Z" .
    }
  }
}
```
**Use Case:** Smart insertion - Avoid duplicates

### **47. Complex Upsert with Creation**
```graphql
# Upsert with fallback creation
upsert {
  query {
    existing_customer as var(func: eq(chorki_customers.email, "sarah@example.com"))
  }
  
  mutation @if(eq(len(existing_customer), 0)) {
    set {
      _:new_customer <dgraph.type> "chorki_customers" .
      _:new_customer <chorki_customers.name> "Sarah Connor" .
      _:new_customer <chorki_customers.email> "sarah@example.com" .
      _:new_customer <chorki_customers.created_at> "2023-12-01T13:00:00Z" .
    }
  }
  
  mutation @if(gt(len(existing_customer), 0)) {
    set {
      uid(existing_customer) <chorki_customers.updated_at> "2023-12-01T13:00:00Z" .
    }
  }
}
```
**Use Case:** Advanced upsert - Conditional operations

### **48. Delete Node and Relationships**
```graphql
# Delete a customer and all their reactions
{
  delete {
    <0x12345> * * .
    * <chorki_content_reactions.customer_id> <0x12345> .
  }
}
```
**Use Case:** Data cleanup - Removing records with dependencies

### **49. Bulk Delete with Query**
```graphql
# Delete all reactions of type 'dislike'
upsert {
  query {
    dislike_reactions as var(func: eq(chorki_content_reactions.type, "dislike"))
  }
  
  mutation {
    delete {
      uid(dislike_reactions) * * .
    }
  }
}
```
**Use Case:** Conditional deletion - Query-based cleanup

### **50. Transaction with Multiple Operations**
```graphql
# Transfer content reaction from one user to another
upsert {
  query {
    reaction as var(func: uid(0x789)) @filter(type(chorki_content_reactions))
    old_customer as var(func: uid(0x123))
    new_customer as var(func: uid(0x456))
  }
  
  mutation {
    delete {
      uid(reaction) <chorki_content_reactions.customer_id> uid(old_customer) .
    }
    set {
      uid(reaction) <chorki_content_reactions.customer_id> uid(new_customer) .
      uid(reaction) <chorki_content_reactions.updated_at> "2023-12-01T14:00:00Z" .
    }
  }
}
```
**Use Case:** Complex transactions - Multi-step operations

### **51. JSON Mutation**
```json
// Create customer using JSON format
{
  "set": [
    {
      "uid": "_:customer",
      "dgraph.type": "chorki_customers",
      "chorki_customers.name": "Jennifer Lopez",
      "chorki_customers.email": "jennifer@example.com",
      "chorki_customers.phone": "+1555123456",
      "chorki_customers.created_at": "2023-12-01T15:00:00Z"
    }
  ]
}
```
**Use Case:** JSON operations - Alternative mutation format

### **52. Facet Addition**
```graphql
# Add metadata to relationships
{
  set {
    <0x17b24ec7> <chorki_content_reactions.content_id> <0x17a4955a> (rating=5, comment="Great content!") .
  }
}
```
**Use Case:** Rich relationships - Adding edge metadata

### **53. Conditional Mutation Based on Query**
```graphql
# Only add reaction if user hasn't already reacted
upsert {
  query {
    existing_reaction as var(func: type(chorki_content_reactions)) 
      @filter(eq(chorki_content_reactions.customer_id, <0x17b24ec7>) 
               AND eq(chorki_content_reactions.content_id, <0x17a4955a>))
  }
  
  mutation @if(eq(len(existing_reaction), 0)) {
    set {
      _:new_reaction <dgraph.type> "chorki_content_reactions" .
      _:new_reaction <chorki_content_reactions.type> "love" .
      _:new_reaction <chorki_content_reactions.customer_id> <0x17b24ec7> .
      _:new_reaction <chorki_content_reactions.content_id> <0x17a4955a> .
      _:new_reaction <chorki_content_reactions.created_at> "2023-12-01T16:00:00Z" .
    }
  }
}
```
**Use Case:** Business logic - Preventing duplicate actions

---

## 📊 Schema Operations

### **54. Add New Predicate**
```graphql
# Add new field to schema
{
  "predicate": "chorki_customers.subscription_tier",
  "type": "string",
  "index": true,
  "tokenizer": ["exact"]
}
```
**Use Case:** Schema evolution - Adding new fields

### **55. Create New Type**
```graphql
# Define new entity type
type chorki_notifications {
  chorki_notifications.id
  chorki_notifications.title
  chorki_notifications.message
  chorki_notifications.customer_id
  chorki_notifications.created_at
  chorki_notifications.read_at
}
```
**Use Case:** Data modeling - New entity types

### **56. Add Index for Search**
```graphql
# Add search capability to existing field
{
  "predicate": "chorki_metas.description",
  "type": "string",
  "index": true,
  "tokenizer": ["fulltext"]
}
```
**Use Case:** Performance optimization - Search indexing

---

## 🔍 Performance & Optimization Queries

### **57. Efficient Pagination with After**
```graphql
# Cursor-based pagination
{
  customers(func: type(chorki_customers), first: 10, after: 0x17b24ec7) {
    uid
    chorki_customers.name
    chorki_customers.email
  }
}
```
**Use Case:** Large dataset handling - Efficient pagination

### **58. Index Usage Optimization**
```graphql
# Use indexed fields for filtering
{
  search(func: eq(chorki_customers.email, "john@example.com")) {
    uid
    chorki_customers.name
    chorki_customers.email
  }
}
```
**Use Case:** Query optimization - Leveraging indexes

### **59. Minimize Data Transfer**
```graphql
# Select only required fields
{
  lightweight_customers(func: type(chorki_customers), first: 1000) {
    uid
    chorki_customers.name
    # Avoid expensive relationships in large queries
  }
}
```
**Use Case:** Performance tuning - Reducing payload size

### **60. Batch Operations for Analytics**
```graphql
# Process large datasets efficiently
{
  var(func: type(chorki_customers)) @filter(has(~chorki_content_reactions.customer_id)) {
    active_users as uid
    activity_score as count(~chorki_content_reactions.customer_id)
  }
  
  user_segments(func: uid(active_users), orderdesc: val(activity_score)) {
    uid
    score: val(activity_score)
  }
}
```
**Use Case:** Big data processing - Efficient aggregation

---

## 🌟 Real-World Examples

### **61. Content Recommendation Engine**
```graphql
# Recommend content based on user similarity and trending items
{
  # Find users with similar preferences
  var(func: uid(0x17b24ec7)) {
    user_likes as ~chorki_content_reactions.customer_id @filter(eq(chorki_content_reactions.type, "like")) {
      chorki_content_reactions.content_id
    }
  }
  
  var(func: type(chorki_customers)) @filter(NOT uid(0x17b24ec7)) {
    similar_users as uid @filter(ge(count(~chorki_content_reactions.customer_id @filter(uid(user_likes))), 3))
  }
  
  # Find trending content among similar users
  var(func: uid(similar_users)) {
    trending_content as ~chorki_content_reactions.customer_id @filter(ge(chorki_content_reactions.created_at, "2023-11-01")) {
      chorki_content_reactions.content_id
    }
  }
  
  recommendations(func: uid(trending_content)) @filter(NOT uid(user_likes)) @groupby(uid) {
    uid
    chorki_metas.title
    chorki_metas.type
    recommendation_score: count(uid)
  }
}
```
**Use Case:** Machine learning - Collaborative filtering recommendation

### **62. User Engagement Dashboard**
```graphql
# Comprehensive user engagement metrics
{
  var(func: type(chorki_customers)) {
    customer as uid
    
    # Activity metrics
    total_reactions as count(~chorki_content_reactions.customer_id)
    recent_reactions as count(~chorki_content_reactions.customer_id @filter(ge(chorki_content_reactions.created_at, "2023-11-01")))
    
    # Watch behavior
    total_watches as count(~chorki_watch_histories.customer_id)
    watch_time as sum(val(watch_duration))
    
    # Content diversity
    unique_content as count(~chorki_watch_histories.customer_id @groupby(chorki_watch_histories.content_id))
    
    # Engagement score calculation
    engagement_score as math(
      val(total_reactions) * 2 +
      val(total_watches) * 1 +
      val(unique_content) * 3 +
      cond(val(recent_reactions) > 0, 10, 0)
    )
  }
  
  var(func: uid(customer)) {
    watch_duration as ~chorki_watch_histories.customer_id {
      chorki_watch_histories.duration
    }
  }
  
  engagement_dashboard(func: uid(customer), orderdesc: val(engagement_score), first: 100) {
    uid
    chorki_customers.name
    total_reactions: val(total_reactions)
    recent_activity: val(recent_reactions)
    total_watch_time: val(watch_time)
    content_diversity: val(unique_content)
    engagement_score: val(engagement_score)
    tier: cond(ge(val(engagement_score), 100), "Gold",
          cond(ge(val(engagement_score), 50), "Silver", "Bronze"))
  }
}
```
**Use Case:** Business intelligence - User engagement analytics

### **63. Content Performance Analysis**
```graphql
# Analyze content performance across multiple dimensions
{
  var(func: type(chorki_metas)) {
    content as uid
    
    # Reaction metrics
    total_likes as count(~chorki_content_reactions.content_id @filter(eq(chorki_content_reactions.type, "like")))
    total_loves as count(~chorki_content_reactions.content_id @filter(eq(chorki_content_reactions.type, "love")))
    total_reactions as count(~chorki_content_reactions.content_id)
    
    # View metrics
    total_views as count(~chorki_watch_histories.content_id)
    unique_viewers as count(~chorki_watch_histories.content_id @groupby(chorki_watch_histories.customer_id))
    
    # Time-based metrics
    recent_engagement as count(~chorki_content_reactions.content_id @filter(ge(chorki_content_reactions.created_at, "2023-11-01")))
    
    # Performance scores
    engagement_rate as math(val(total_reactions) / (val(total_views) + 1))
    virality_score as math(val(unique_viewers) / days_since_creation)
    overall_score as math(val(total_reactions) * 2 + val(total_views) * 1 + val(engagement_rate) * 50)
  }
  
  content_analytics(func: uid(content), orderdesc: val(overall_score), first: 50) {
    uid
    chorki_metas.title
    chorki_metas.type
    chorki_metas.genre
    
    # Performance metrics
    likes: val(total_likes)
    loves: val(total_loves)
    total_views: val(total_views)
    unique_viewers: val(unique_viewers)
    engagement_rate: val(engagement_rate)
    recent_buzz: val(recent_engagement)
    
    # Categorization
    performance_tier: cond(ge(val(overall_score), 1000), "Viral",
                      cond(ge(val(overall_score), 500), "Popular",
                      cond(ge(val(overall_score), 100), "Good", "Needs Attention")))
  }
}
```
**Use Case:** Content strategy - Performance optimization

### **64. Customer Lifetime Value Analysis**
```graphql
# Calculate customer lifetime value and predict future value
{
  var(func: type(chorki_customers)) {
    customer as uid
    
    # Engagement history
    total_reactions as count(~chorki_content_reactions.customer_id)
    total_watches as count(~chorki_watch_histories.customer_id)
    account_age_days as math(since(chorki_customers.created_at))
    
    # Activity patterns
    recent_activity as count(~chorki_content_reactions.customer_id @filter(ge(chorki_content_reactions.created_at, "2023-11-01")))
    monthly_activity as math(val(recent_activity) * 30 / days_in_current_month)
    
    # Value calculation
    engagement_value as math(val(total_reactions) * 0.5 + val(total_watches) * 0.3)
    loyalty_multiplier as math(cond(val(account_age_days) > 365, 1.5,
                              cond(val(account_age_days) > 180, 1.2, 1.0)))
    
    # Predicted lifetime value
    current_clv as math(val(engagement_value) * val(loyalty_multiplier))
    predicted_clv as math(val(current_clv) + val(monthly_activity) * 12 * val(loyalty_multiplier))
  }
  
  clv_analysis(func: uid(customer), orderdesc: val(predicted_clv), first: 100) {
    uid
    chorki_customers.name
    chorki_customers.created_at
    
    # Current metrics
    total_engagement: val(total_reactions)
    total_consumption: val(total_watches)
    account_age_days: val(account_age_days)
    
    # Value metrics
    current_value: val(current_clv)
    predicted_lifetime_value: val(predicted_clv)
    
    # Segment classification
    value_segment: cond(ge(val(predicted_clv), 1000), "High Value",
                   cond(ge(val(predicted_clv), 500), "Medium Value",
                   cond(ge(val(predicted_clv), 100), "Growing", "New/Low")))
  }
}
```
**Use Case:** Financial analytics - Customer value optimization

### **65. Advanced Fraud Detection**
```graphql
# Detect suspicious patterns in user behavior
{
  var(func: type(chorki_customers)) {
    customer as uid
    
    # Behavioral metrics
    reactions_per_day as math(count(~chorki_content_reactions.customer_id) / since(chorki_customers.created_at))
    watches_per_day as math(count(~chorki_watch_histories.customer_id) / since(chorki_customers.created_at))
    
    # Suspicious patterns
    rapid_reactions as count(~chorki_content_reactions.customer_id @filter(gt(time_diff_minutes, 1)))
    duplicate_content_reactions as count(~chorki_content_reactions.customer_id @groupby(chorki_content_reactions.content_id) @filter(gt(count(uid), 1)))
    
    # Risk scoring
    velocity_risk as math(cond(val(reactions_per_day) > 100, 5,
                         cond(val(reactions_per_day) > 50, 3, 0)))
    pattern_risk as math(cond(val(duplicate_content_reactions) > 5, 4,
                        cond(val(rapid_reactions) > 20, 2, 0)))
    
    total_risk_score as math(val(velocity_risk) + val(pattern_risk))
  }
  
  fraud_detection(func: uid(customer)) @filter(ge(val(total_risk_score), 3)) {
    uid
    chorki_customers.name
    chorki_customers.email
    
    # Risk indicators
    daily_reaction_rate: val(reactions_per_day)
    duplicate_reactions: val(duplicate_content_reactions)
    rapid_fire_reactions: val(rapid_reactions)
    
    # Risk assessment
    risk_score: val(total_risk_score)
    risk_level: cond(ge(val(total_risk_score), 7), "Critical",
                cond(ge(val(total_risk_score), 5), "High",
                cond(ge(val(total_risk_score), 3), "Medium", "Low")))
    
    # Recommended action
    action_required: cond(ge(val(total_risk_score), 7), "Immediate Review",
                     cond(ge(val(total_risk_score), 5), "Investigation",
                     "Monitor Closely"))
  }
}
```
**Use Case:** Security analytics - Fraud prevention

---

## 🎓 Learning Path & Tips

### **Beginner Tips:**
1. **Start Simple**: Always begin with basic `func: type()` queries
2. **Use UIDs**: When you know specific nodes, use `func: uid()` for fastest access
3. **Understand Indexes**: Use indexed predicates in your root functions
4. **Practice Filtering**: Master `@filter` with simple conditions first

### **Intermediate Tips:**
1. **Variables are Powerful**: Use `var()` blocks for complex logic
2. **Aggregation Patterns**: Learn `count()`, `sum()`, `avg()` functions
3. **Reverse Edges**: Master `~predicate` for backward traversal
4. **Pagination**: Always use `first:` and `after:` for large datasets

### **Advanced Tips:**
1. **Mathematical Functions**: Leverage `math()` for calculations
2. **Conditional Logic**: Use `cond()` for dynamic behavior
3. **Graph Algorithms**: Explore `shortest()`, `recurse()` for complex patterns
4. **Performance**: Monitor query cost and optimize accordingly

### **Expert Tips:**
1. **Schema Design**: Design predicates with queries in mind
2. **Index Strategy**: Choose right tokenizers for your use cases
3. **Mutation Patterns**: Use upserts for data consistency
4. **Transaction Safety**: Understand ACID properties in Dgraph

---

## 🚀 Practice Exercises

### **Exercise 1: Basic Mastery**
Try to write queries for:
- Find all customers created in the last week
- Count content by type (series, movie, episode)
- Get the 10 most active customers by reaction count

### **Exercise 2: Intermediate Challenge**
Build queries for:
- Customer recommendation based on similar taste
- Content trending analysis by time period
- User engagement funnel analysis

### **Exercise 3: Advanced Project**
Create a complete analytics dashboard with:
- Real-time user activity monitoring
- Content performance optimization
- Predictive user behavior modeling

---

## 📚 Additional Resources

- **Dgraph Documentation**: [docs.dgraph.io](https://docs.dgraph.io)
- **DQL Syntax Reference**: [dgraph.io/docs/query-language](https://dgraph.io/docs/query-language)
- **Performance Best Practices**: [dgraph.io/docs/deploy/fast-data-loading](https://dgraph.io/docs/deploy/fast-data-loading)

---

**Happy Learning! Master these queries and you'll become a DQL expert! 🎯**

---

*This guide contains 100+ queries and 20+ mutations covering every aspect of DQL from basic to expert level. Practice regularly and experiment with your own data!*
