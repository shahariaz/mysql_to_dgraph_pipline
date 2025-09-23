# DQL Query to Find Users Who Watched or Liked Shakib Khan Movies

Based on your chorki database schema, here's the comprehensive query to find users who either:
1. Watched Shakib Khan movies for more than 30 seconds, OR
2. Liked Shakib Khan movies

```graphql
# Query to find users who watched or liked Shakib Khan content
{
  # Step 1: Find Shakib Khan as a cast in metadata
  var(func: anyofterms(chorki_metas.title, "Shakib Khan")) @filter(eq(chorki_metas.type, "casts")) {
    shakib_cast_meta as uid
  }
  
  # Step 2: Find series that have Shakib Khan as cast
  var(func: type(chorki_series_meta)) @filter(uid_in(chorki_series_meta.meta_id, shakib_cast_meta)) {
    shakib_series_links as chorki_series_meta.series_id
  }
  
  # Step 3: Find videos belonging to Shakib Khan series
  var(func: type(chorki_videos)) @filter(uid_in(chorki_videos.series_id, shakib_series_links)) {
    shakib_series_videos as uid
  }
  
  # Step 4: Also find videos that have Shakib Khan directly in video metadata
  var(func: type(chorki_video_meta)) @filter(uid_in(chorki_video_meta.meta_id, shakib_cast_meta)) {
    shakib_video_links as chorki_video_meta.video_id
  }
  
  # Step 5: Alternative - Find content with "Shakib" in titles
  var(func: anyofterms(chorki_videos.title, "Shakib")) {
    shakib_title_videos as uid
  }
  
  # Step 6: Combine all Shakib Khan videos
  var(func: uid(shakib_series_videos, shakib_video_links, shakib_title_videos)) {
    all_shakib_videos as uid
  }
  
  # Step 7: Find users who watched Shakib Khan videos for more than 30 seconds
  var(func: type(chorki_watch_histories.sql)) @filter(uid_in(chorki_watch_histories.sql.content_id, all_shakib_videos) AND ge(chorki_watch_histories.sql.watched_duration, 30000)) {
    long_watchers as chorki_watch_histories.sql.customer_id
  }
  
  # Step 8: Find users who liked Shakib Khan videos
  var(func: type(chorki_content_reactions)) @filter(uid_in(chorki_content_reactions.content_id, all_shakib_videos) AND eq(chorki_content_reactions.type, "like")) {
    content_likers as chorki_content_reactions.customer_id
  }
  
  # Final result: Users who either watched for 30+ seconds OR liked Shakib Khan content
  shakib_khan_fans(func: uid(long_watchers, content_likers)) {
    uid
    chorki_customers.name
    chorki_customers.email
    
    # Show their watch history for Shakib Khan content
    watch_history: ~chorki_watch_histories.sql.customer_id @filter(uid_in(chorki_watch_histories.sql.content_id, all_shakib_videos)) {
      chorki_watch_histories.sql.watched_duration
      chorki_watch_histories.sql.watched_at
      content: chorki_watch_histories.sql.content_id {
        chorki_videos.title
        chorki_videos.episode_number
        series: chorki_videos.series_id {
          chorki_series.title
        }
      }
    }
    
    # Show their reactions to Shakib Khan content
    reactions: ~chorki_content_reactions.customer_id @filter(uid_in(chorki_content_reactions.content_id, all_shakib_videos)) {
      chorki_content_reactions.type
      chorki_content_reactions.created_at
      reacted_content: chorki_content_reactions.content_id {
        chorki_videos.title
        chorki_videos.episode_number
        series: chorki_videos.series_id {
          chorki_series.title
        }
      }
    }
    
    # Calculate engagement metrics
    total_shakib_watches: count(~chorki_watch_histories.sql.customer_id @filter(uid_in(chorki_watch_histories.sql.content_id, all_shakib_videos)))
    total_shakib_reactions: count(~chorki_content_reactions.customer_id @filter(uid_in(chorki_content_reactions.content_id, all_shakib_videos)))
  }
}
```

## Simplified Version (Easier to Test)

If the above complex query doesn't work due to schema variations, here's a simpler approach:

```graphql
# Simplified query focusing on videos with Shakib in title or cast
{
  # Find videos with Shakib in title
  var(func: anyofterms(chorki_videos.title, "Shakib")) {
    shakib_videos as uid
  }
  
  # Users who watched Shakib videos for 30+ seconds
  var(func: type(chorki_watch_histories.sql)) @filter(
    uid_in(chorki_watch_histories.sql.content_id, shakib_videos) 
    AND ge(chorki_watch_histories.sql.watched_duration, 30000)
  ) {
    watchers as chorki_watch_histories.sql.customer_id
  }
  
  # Users who liked Shakib videos
  var(func: type(chorki_content_reactions)) @filter(
    uid_in(chorki_content_reactions.content_id, shakib_videos)
    AND eq(chorki_content_reactions.type, "like")
  ) {
    likers as chorki_content_reactions.customer_id
  }
  
  # Combined results
  shakib_fans(func: uid(watchers, likers)) {
    uid
    chorki_customers.name
    chorki_customers.email
    
    # Watch statistics
    total_watches: count(~chorki_watch_histories.sql.customer_id)
    total_reactions: count(~chorki_content_reactions.customer_id)
    
    # Specific Shakib content interactions
    shakib_watches: count(~chorki_watch_histories.sql.customer_id @filter(
      uid_in(chorki_watch_histories.sql.content_id, shakib_videos)
    ))
    
    shakib_likes: count(~chorki_content_reactions.customer_id @filter(
      uid_in(chorki_content_reactions.content_id, shakib_videos)
      AND eq(chorki_content_reactions.type, "like")
    ))
  }
}
```

## Ultra Simple Version (For Testing)

```graphql
# Start with this simple version to test videos only
{
  # Find any videos mentioning Shakib
  shakib_videos(func: anyofterms(chorki_videos.title, "Shakib"), first: 10) {
    uid
    chorki_videos.title
    chorki_videos.episode_number
    
    # Show series information
    series: chorki_videos.series_id {
      chorki_series.title
    }
    
    # Users who liked this video
    fans: ~chorki_content_reactions.content_id @filter(eq(chorki_content_reactions.type, "like")) {
      chorki_content_reactions.customer_id {
        uid
        chorki_customers.name
      }
    }
    
    # Users who watched this video for 30+ seconds
    long_watchers: ~chorki_watch_histories.sql.content_id @filter(ge(chorki_watch_histories.sql.watched_duration, 30000)) {
      chorki_watch_histories.sql.customer_id {
        uid
        chorki_customers.name
      }
      chorki_watch_histories.sql.watched_duration
    }
  }
}
```

## Test Query for Cast Metadata (Shakib Khan as Actor)

```graphql
# Test to find Shakib Khan in cast metadata
{
  # Find Shakib Khan in metadata
  shakib_cast(func: anyofterms(chorki_metas.title, "Shakib Khan"), first: 5) {
    uid
    chorki_metas.title
    chorki_metas.type
    
    # Find series linked to this cast
    series: ~chorki_series_meta.meta_id {
      chorki_series_meta.series_id {
        uid
        chorki_series.title
        
        # Videos in this series
        videos: ~chorki_videos.series_id {
          uid
          chorki_videos.title
          chorki_videos.episode_number
          
          # Watchers of these videos
          watchers: ~chorki_watch_histories.sql.content_id @filter(ge(chorki_watch_histories.sql.watched_duration, 30000)) (first: 3) {
            chorki_watch_histories.sql.customer_id {
              chorki_customers.name
            }
            chorki_watch_histories.sql.watched_duration
          }
        }
      }
    }
    
    # Find videos directly linked to this cast
    direct_videos: ~chorki_video_meta.meta_id {
      chorki_video_meta.video_id {
        uid
        chorki_videos.title
        
        # Watchers of these videos
        watchers: ~chorki_watch_histories.sql.content_id @filter(ge(chorki_watch_histories.sql.watched_duration, 30000)) (first: 3) {
          chorki_watch_histories.sql.customer_id {
            chorki_customers.name
          }
          chorki_watch_histories.sql.watched_duration
        }
      }
    }
  }
}
```

## Usage Instructions:

1. **Start with the Ultra Simple Version** to test if the relationships work
2. **Gradually build up** to the more complex versions
3. **Adjust field names** based on your actual schema (watch_histories vs watch_histories.sql)
4. **Modify time threshold** from 30000 (30 seconds) to your preferred duration
5. **Add more content types** like episodes, movies if they exist in your data

Try running these queries in order of complexity until you find one that works with your specific data structure!