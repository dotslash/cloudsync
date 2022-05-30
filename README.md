2 way sync between Google Cloud Storage (the s3 equivalent for GCP) and a local machine. There are probably other tools
that do similar things. Im doing it for fun.

### Current state of things and what's next

This is a loop forever that step1) 2 syncs data from local machine and gcs. step2) sleep 30 secs. This works more or
less, except that there are atleast a few things to improve

* While a making GCP apis once in 30 secs is okay, it seems wrong. Dont have a good explanation yet
* No pagination. I think the GCP SDK i use takes care of that, if the directory is large, i will hold it all in memory.
  Is this okay?
* If i and start the sync process, it plays safe and removed files will be added back. I can save the last scan state on
  disk to avoid this.
* No unit or integration tests. The only testing i did was to sync this repo by using the code here to GCS
    - `go run *go -remote=gs://<my gcp bucket>/cloudsync -local=$PWD`
* support gitignore. E.g in my testing i would have liked to skip the .git directory and .idea directory
  <img src="https://storage.googleapis.com/yesteapea/9d120347-181b-4d0a-86f5-876c5ad52745.png">
* Support trash
* Support recovering from an earlier state. Is it possible to give a simple experience? - Something like "give me state
  of things as of <time> from the cloud". Maybe that's too much.
* Should we preserve blob attributes when overwriting content. E.g if the blob is public before, maybe we should make
  the overwrite public too
* Should we do blobstore operations in parallel?
* Should we do local file operations in parallel?

There might be more things to do.
