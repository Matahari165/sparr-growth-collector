"""YouTube collector: public stats for every video of one channel -> Neon.

Env: YOUTUBE_API_KEY, NEON_DATABASE_URL, optional YOUTUBE_HANDLE (default @sparr.life49),
YOUTUBE_ACCOUNT_TYPE (default other).
Cost per run: about 1 + ceil(videos/50) * 2 quota units (daily budget is 10,000).
"""
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

import psycopg

API = "https://www.googleapis.com/youtube/v3"
HOURLY_WINDOW_DAYS = 7  # younger videos get a snapshot every run, older ones once a day


def call(path, **params):
    params["key"] = os.environ["YOUTUBE_API_KEY"]
    url = f"{API}/{path}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        # the key is in the URL: never print it
        body = e.read().decode("utf-8", "replace")[:500]
        sys.exit(f"YouTube API error {e.code} on {path}: {body}")


def parse_duration(iso):
    """ISO 8601 like PT1M5S -> seconds."""
    m = re.fullmatch(r"P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "")
    if not m:
        return None
    d, h, mi, s = (int(x or 0) for x in m.groups())
    return d * 86400 + h * 3600 + mi * 60 + s


def to_int(value):
    return int(value) if value is not None else None


def main():
    handle = os.environ.get("YOUTUBE_HANDLE", "@sparr.life49")
    account_type = os.environ.get("YOUTUBE_ACCOUNT_TYPE", "other")

    ch = call("channels", part="snippet,contentDetails,statistics", forHandle=handle)
    if not ch.get("items"):
        sys.exit(f"No YouTube channel found for {handle}")
    channel = ch["items"][0]
    channel_id = channel["id"]
    uploads = channel["contentDetails"]["relatedPlaylists"]["uploads"]
    followers = to_int(channel["statistics"].get("subscriberCount"))

    video_ids, page = [], None
    while True:
        params = dict(part="contentDetails", playlistId=uploads, maxResults=50)
        if page:
            params["pageToken"] = page
        res = call("playlistItems", **params)
        video_ids += [i["contentDetails"]["videoId"] for i in res["items"]]
        page = res.get("nextPageToken")
        if not page:
            break

    with psycopg.connect(os.environ["NEON_DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO accounts (platform, handle, account_type, external_id)
               VALUES ('youtube', %s, %s, %s)
               ON CONFLICT (platform, handle) DO UPDATE SET external_id = EXCLUDED.external_id
               RETURNING id""",
            (handle, account_type, channel_id),
        )
        account_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO account_snapshots (account_id, followers) VALUES (%s, %s)",
            (account_id, followers),
        )

        taken = 0
        for i in range(0, len(video_ids), 50):
            res = call("videos", part="snippet,contentDetails,statistics",
                       id=",".join(video_ids[i:i + 50]))
            for v in res["items"]:
                duration = parse_duration(v["contentDetails"].get("duration"))
                fmt = "short" if duration is not None and duration <= 180 else "long"
                cur.execute(
                    """INSERT INTO videos (account_id, platform_video_id, url, caption,
                                           posted_at, duration_s, format)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (account_id, platform_video_id)
                       DO UPDATE SET caption = EXCLUDED.caption, duration_s = EXCLUDED.duration_s
                       RETURNING id, posted_at""",
                    (account_id, v["id"], f"https://www.youtube.com/watch?v={v['id']}",
                     v["snippet"]["title"], v["snippet"]["publishedAt"], duration, fmt),
                )
                video_id, posted_at = cur.fetchone()
                st = v["statistics"]
                cur.execute(
                    """INSERT INTO snapshots (video_id, views, likes, comments)
                       SELECT %s, %s, %s, %s
                       WHERE %s > now() - make_interval(days => %s)
                          OR NOT EXISTS (SELECT 1 FROM snapshots
                                         WHERE video_id = %s
                                           AND captured_at > now() - interval '23 hours')""",
                    (video_id, to_int(st.get("viewCount")), to_int(st.get("likeCount")),
                     to_int(st.get("commentCount")), posted_at, HOURLY_WINDOW_DAYS, video_id),
                )
                taken += cur.rowcount

        cur.execute("SELECT detect_outliers(3)")
        alerts = cur.fetchone()[0]
        conn.commit()

    print(f"{handle}: {len(video_ids)} videos seen, {taken} snapshots saved, "
          f"{followers} subscribers, {alerts} new outlier alerts")


if __name__ == "__main__":
    # This repository is public and its logs are public: never print driver error details,
    # which can contain the database host or user name.
    try:
        main()
    except psycopg.Error as e:
        sys.exit(f"Database error: {type(e).__name__}")
