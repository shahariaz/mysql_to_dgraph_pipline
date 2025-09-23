# Generic MySQL to Dgraph Pipeline

This tool is designed to work with **ANY MySQL database** and automatically convert it to Dgraph format. It doesn't require any hardcoded table names or relationships.

## ✨ Generic Features

### 🔗 Smart Foreign Key Detection

The pipeline automatically detects foreign key relationships using multiple strategies:

1. **Explicit FK Constraints**: Reads actual MySQL foreign key constraints
2. **Naming Convention Detection**: Supports multiple FK naming patterns:
   - `*_id` → Most common (user_id, customer_id, product_id)
   - `*_key` → Alternative pattern (user_key, product_key)
   - `*_ref` → Reference pattern (user_ref, product_ref)
   - `id_*` → Reverse pattern (id_user, id_product)
   - `fk_*` → Explicit FK prefix (fk_user, fk_product)

3. **Table Name Matching**: Automatically finds referenced tables:
   - Direct match: `user_id` → `user`
   - Plural forms: `user_id` → `users`
   - Complex plurals: `category_id` → `categories`
   - Self-referential: `parent_id` → same table

4. **Prefix Detection**: Automatically detects and handles table prefixes:
   - If you have `app_users`, `app_posts`, `app_comments`
   - It detects `app_` as a common prefix
   - Maps `user_id` → `app_users`, `post_id` → `app_posts`

### 📊 Universal Schema Support

Works with any MySQL database structure:
- E-commerce databases (products, orders, customers)
- CMS databases (posts, categories, users)
- Social media databases (users, posts, comments, likes)
- Financial databases (accounts, transactions, users)
- Any custom database schema

## 🚀 Usage Examples

### Example 1: E-commerce Database

```yaml
# config/ecommerce.yaml
mysql:
  host: "localhost"
  port: 3306
  user: "ecommerce_user"
  password: "password"
  database: "ecommerce_db"
```

Tables: `users`, `products`, `orders`, `order_items`, `categories`

**Automatic FK Detection:**
- `orders.user_id` → `users.id`
- `order_items.order_id` → `orders.id`
- `order_items.product_id` → `products.id`
- `products.category_id` → `categories.id`

```bash
./pipeline -config=config/ecommerce.yaml -mode=full
```

### Example 2: Blog/CMS Database

```yaml
# config/blog.yaml
mysql:
  host: "localhost"
  port: 3306
  user: "blog_user"
  password: "password"
  database: "blog_db"
```

Tables: `users`, `posts`, `comments`, `categories`, `tags`, `post_tags`

**Automatic FK Detection:**
- `posts.user_id` → `users.id`
- `posts.category_id` → `categories.id`
- `comments.post_id` → `posts.id`
- `comments.user_id` → `users.id`
- `post_tags.post_id` → `posts.id`
- `post_tags.tag_id` → `tags.id`

```bash
./pipeline -config=config/blog.yaml -mode=full
```

### Example 3: Prefixed Tables Database

```yaml
# config/app.yaml
mysql:
  host: "localhost"
  port: 3306
  user: "app_user"
  password: "password"
  database: "app_db"
```

Tables: `app_users`, `app_posts`, `app_comments`, `app_categories`

**Automatic Prefix Detection & FK Mapping:**
- Detects `app_` as common prefix
- `app_posts.user_id` → `app_users.id`
- `app_comments.post_id` → `app_posts.id`
- `app_posts.category_id` → `app_categories.id`

```bash
./pipeline -config=config/app.yaml -mode=full
```

## 🔧 Command Line Options

### Basic Usage
```bash
# Full pipeline (schema + data + validation)
./pipeline -config=config/your_db.yaml

# Schema extraction only
./pipeline -config=config/your_db.yaml -mode=schema

# Data migration only
./pipeline -config=config/your_db.yaml -mode=data

# Specific tables only
./pipeline -config=config/your_db.yaml -tables="users,posts,comments"

# Dry run (preview what will be done)
./pipeline -config=config/your_db.yaml -dry-run

# Performance tuning
./pipeline -config=config/your_db.yaml -parallel=8 -batch-size=5000
```

### Configuration File Template

```yaml
# config/your_database.yaml
mysql:
  host: "your_mysql_host"
  port: 3306
  user: "your_username"
  password: "your_password"
  database: "your_database_name"
  max_connections: 10
  timeout: "30s"

pipeline:
  workers: 4                    # Parallel workers
  batch_size: 1000             # Records per batch
  memory_limit_mb: 1024        # Memory limit
  
output:
  directory: "output"
  rdf_file: "data.rdf"
  schema_file: "schema.txt"
  mapping_file: "uid_mapping.txt"

logger:
  level: "info"               # debug, info, warn, error
  format: "json"              # json, text
```

## 📈 Performance Guidelines

### Small Databases (<1M records)
```bash
./pipeline -config=config.yaml -parallel=2 -batch-size=500
```

### Medium Databases (1M-100M records)
```bash
./pipeline -config=config.yaml -parallel=4 -batch-size=1000
```

### Large Databases (100M+ records)
```bash
./pipeline -config=config.yaml -parallel=8 -batch-size=5000
```

## 🎯 Output Files

After running the pipeline, you'll get:

1. **`data.rdf`**: Complete RDF data with relationships
2. **`schema.txt`**: Dgraph schema with predicates and types
3. **`uid_mapping.txt`**: UID mappings for reference
4. **`checkpoint.json`**: Progress checkpoints (for resume capability)

## 🔄 Import to Dgraph

```bash
# Using Dgraph live loader
dgraph live -f output/data.rdf -s output/schema.txt --alpha localhost:9080 --zero localhost:5080

# Using Docker
docker run --rm -v $(pwd)/output:/data dgraph/dgraph:latest dgraph live -f /data/data.rdf -s /data/schema.txt --alpha dgraph-alpha:9080 --zero dgraph-zero:5080
```

## 🔍 Query Examples

After import, your data can be queried with DQL:

### Find all users and their posts
```dql
{
  users(func: type(users)) {
    users.name
    users.email
    posts: ~posts.user_id {
      posts.title
      posts.content
    }
  }
}
```

### Find posts with comments
```dql
{
  posts(func: type(posts)) {
    posts.title
    posts.content
    comments: ~comments.post_id {
      comments.content
      author: comments.user_id {
        users.name
      }
    }
  }
}
```

## ✅ Validation

The tool includes automatic validation:
- Foreign key integrity checks
- Data type validation
- Relationship consistency verification
- Missing reference detection

```bash
# Run validation only
./pipeline -config=config.yaml -mode=validate
```

## 🚨 Troubleshooting

### Connection Issues
```bash
# Test MySQL connection
mysql -h your_host -u your_user -p your_database

# Check config file path
./pipeline -config=path/to/your/config.yaml -dry-run
```

### Memory Issues
```bash
# Reduce batch size and workers
./pipeline -config=config.yaml -batch-size=100 -parallel=1
```

### Missing Relationships
- Check MySQL foreign key constraints: `SHOW CREATE TABLE your_table`
- Verify naming conventions match supported patterns
- Use debug logging: `logger.level: "debug"` in config

## 🎉 Success!

Your MySQL database is now ready to use with Dgraph! The tool has automatically:
- ✅ Detected all table relationships
- ✅ Converted data to RDF format  
- ✅ Generated optimized Dgraph schema
- ✅ Created proper indexes and types
- ✅ Maintained data integrity

Start querying your graph database with the power of DQL! 🚀