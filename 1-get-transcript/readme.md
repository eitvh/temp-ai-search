YouTube Transcript V2 Process Flow

Overview
This script fetches YouTube captions from TikHub, saves transcript JSON files to S3, stores S3 URLs in MySQL, and updates srt_id in MongoDB. It is designed to continue safely from checkpoint files if the process stops.

Main Sources and Outputs
- Source video_id: MongoDB collection from MONGO_COLL_ATLAS_YOUTUBE
- TikHub endpoint: /api/v1/youtube/web_v2/get_video_captions_v2
- Local checkpoint file: youtube_transcript_s3_v2_checkpoint.json
- Local transcript backup folder: youtube_transcript_v2_payloads/
- S3 output file: {AWS_PREFIX_TRANSCRIPT}/{video_id}.json
- MySQL table: ai_yt_info_v2
- MongoDB update field: srt_id

Process Flow
1. Load the local checkpoint file.
2. Resume from last_mongo_id, so the script does not restart from the beginning.
3. Read video_id from MongoDB using _id pagination.
4. For each video_id, call TikHub captions API with:
   - video_id
   - language_code=en
   - format=srt
5. If TikHub returns HTTP 500 or above, stop the whole process immediately.
6. Build transcript payload from TikHub response:
   - video_id
   - language_code
   - language_name
   - format
   - content
   - error
7. Save the full payload locally first in youtube_transcript_v2_payloads/.
8. Save checkpoint immediately after local payload is created.
9. Add the item to pending_s3 in the checkpoint.
10. Every 100 records, flush pending items:
    - Upload transcript JSON to S3.
    - Bulk upsert MySQL rows.
    - Bulk update MongoDB srt_id.
11. If S3 fails, the item stays in pending_s3 and retries on the next flush.
12. If MySQL fails, the item stays in pending_mysql and retries on the next flush.
13. If MongoDB update fails, the item stays in pending_mongo and retries on the next flush.
14. After MongoDB update succeeds, the local payload file can be deleted.
15. At the end, run a final flush for remaining pending items.

srt_id Mapping
- 1 = TikHub returned language_code "en" with caption content.
- 2 = TikHub returned language_code "a.en" with caption content.
- 3 = No caption, no content, or non-200 TikHub response.
- 0 = Other language_code with caption content.

MySQL Table
Table name: ai_yt_info_v2
Saved fields:
- id
- video_id
- language_code
- s3_url
- error
- created_at
- updated_at

Checkpoint Behavior
The checkpoint file stores progress and pending queues. It does not store the full transcript content. Full transcript content is backed up in local payload JSON files before any S3 or MySQL write happens.

Important Checkpoint Fields
- last_mongo_id: MongoDB _id used for resume.
- last_video_id: latest processed video_id.
- pending_s3: items waiting for S3 upload.
- pending_mysql: items waiting for MySQL insert/update.
- pending_mongo: items waiting for MongoDB srt_id update.
- run_processed: processed count for current run.
- cumulative_processed: total processed count across runs.
- status: current process status.

Safe Resume
If the script stops, run it again. It will load the checkpoint and continue from last_mongo_id. Pending S3, MySQL, and MongoDB items will retry automatically.

Common Status Values
- START: script started.
- FETCHING_TIKHUB: fetching captions from TikHub.
- CHECKPOINTED: local payload and checkpoint were saved.
- FLUSHING_PENDING: pending records are being written to S3/MySQL/MongoDB.
- FLUSHED_BATCH: one batch flush completed.
- DONE: process completed.
- DONE_WITH_ERRORS: process completed but some errors happened.
- DONE_WITH_PENDING: some pending items still need retry.
- STOPPED_TIKHUB_500: process stopped because TikHub returned server error.
