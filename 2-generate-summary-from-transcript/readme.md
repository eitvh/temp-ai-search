Start
  ↓
Load .env
  ↓
Use Gemini model
  - default: gemini-3.1-flash-lite
  - override with GEMINI_MODEL
  ↓
Read checkpoint file
  - ai_yt_info_v2_summary_checkpoint.json
  ↓
Flush pending_mysql first
  - checkpoint data is written before MySQL insert
  - if MySQL fails, stop immediately
  ↓
Read source rows from MySQL table
  - source table: ai_yt_info_v2
  - video_id column: video_id
  - language column: language_code
  - S3 URL column: s3_url
  - filter: language_code IN ('en', 'a.en')
  - filter: s3_url is not empty
  - batch size: READ_BATCH_SIZE
  ↓
For each video_id
  ↓
Check summary table
  - table: ai_yt_info_v2_summary
  - if video_id already exists and FORCE_REPROCESS=false, skip
  ↓
Read transcript JSON from S3
  - uses s3_url from ai_yt_info_v2
  - transcript field: content
  ↓
Call Gemini
  ↓
If Gemini/model/network/API error happens
  - save error into checkpoint
  - do not insert empty MySQL row
  - stop process immediately
  ↓
If Gemini succeeds
  - checkpoint output first into pending_mysql
  ↓
Insert into MySQL table
  - ai_yt_info_v2_summary
  - fields:
    - video_id
    - video_title
    - brand_mentioned
    - product_mentioned
    - topic_tags
    - summary
    - key_topics
    - is_sponsorship
    - sponsorship_name
    - error
  ↓
Clear pending_mysql after successful insert
  ↓
Update checkpoint counters
  ↓
Continue next video
  ↓
End
