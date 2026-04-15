#!/usr/bin/env bash
#
# One-time setup for the SA/SB worker infrastructure on GCP.
#
# Creates two Cloud Run Jobs (small + big) and two Cloud Scheduler
# triggers, then verifies the IAM grants needed for them to run.
#
# Idempotent: safe to re-run. Existing resources are detected and skipped
# (the GitHub Actions deploy workflow takes over from there for image
# updates).
#
# Usage:
#   chmod +x scripts/setup_sa_sb_jobs.sh
#   ./scripts/setup_sa_sb_jobs.sh
#
# Prerequisites:
#   - gcloud auth login (with a user that has owner/admin on freeway2026)
#   - The fec-ingest-job already exists (we read its runtime SA + image)
#
set -euo pipefail

# -------- config --------
PROJECT_ID="freeway2026"
REGION="us-central1"
EXISTING_JOB="fec-ingest-job"
SMALL_JOB="fec-sa-sb-job"
BIG_JOB="fec-sa-sb-job-big"
SMALL_TRIGGER="fec-sa-sb-trigger"
BIG_TRIGGER="fec-sa-sb-big-trigger"
SMALL_SCHEDULE="*/5 * * * *"      # every 5 min
BIG_SCHEDULE="*/15 * * * *"        # every 15 min
JOB_RUNNER_SA=""                   # auto-detected from EXISTING_JOB

# -------- helpers --------
log() { echo -e "\033[1;34m[setup]\033[0m $*"; }
warn() { echo -e "\033[1;33m[warn]\033[0m $*" >&2; }
die() { echo -e "\033[1;31m[fatal]\033[0m $*" >&2; exit 1; }

# -------- pre-flight --------
log "Checking gcloud auth + project..."
gcloud config set project "$PROJECT_ID" >/dev/null
ACTIVE_USER="$(gcloud config get-value account 2>/dev/null || true)"
[ -n "$ACTIVE_USER" ] || die "no active gcloud account; run 'gcloud auth login'"
log "  active account: $ACTIVE_USER"
log "  project:        $PROJECT_ID"

log "Checking that $EXISTING_JOB exists (we copy its runtime SA + image)..."
if ! gcloud run jobs describe "$EXISTING_JOB" --region="$REGION" >/dev/null 2>&1; then
    die "$EXISTING_JOB not found in $REGION. This script is meant to run after Phase 1 deploy is healthy."
fi

JOB_RUNNER_SA="$(gcloud run jobs describe "$EXISTING_JOB" \
    --region="$REGION" \
    --format='value(spec.template.spec.template.spec.serviceAccountName)')"
if [ -z "$JOB_RUNNER_SA" ]; then
    PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
    JOB_RUNNER_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
    warn "no explicit SA on $EXISTING_JOB; assuming default Compute SA: $JOB_RUNNER_SA"
fi
log "  runtime SA: $JOB_RUNNER_SA"

EXISTING_IMAGE="$(gcloud run jobs describe "$EXISTING_JOB" \
    --region="$REGION" \
    --format='value(spec.template.spec.template.spec.containers[0].image)')"
[ -n "$EXISTING_IMAGE" ] || die "couldn't read image from $EXISTING_JOB"
log "  current image: $EXISTING_IMAGE"

# -------- IAM verification --------
log ""
log "Verifying runtime SA permissions..."
RUNTIME_POLICY="$(gcloud projects get-iam-policy "$PROJECT_ID" \
    --flatten="bindings[].members" \
    --filter="bindings.members:$JOB_RUNNER_SA" \
    --format="value(bindings.role)" 2>/dev/null || true)"

check_role() {
    local role="$1"
    local why="$2"
    if echo "$RUNTIME_POLICY" | grep -qx "$role"; then
        log "  ✓ runtime SA has $role  ($why)"
    else
        warn "  ✗ runtime SA missing $role  ($why)"
        warn "    grant with: gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$JOB_RUNNER_SA --role=$role"
        return 1
    fi
}

PERM_OK=true
check_role "roles/cloudsql.client" "connect to fec-db" || PERM_OK=false
check_role "roles/artifactregistry.reader" "pull image" || PERM_OK=false
[ "$PERM_OK" = "true" ] || warn "fix the missing roles above before running the jobs (script will continue creating the resources)"

# -------- Create / update the two jobs --------
read -r -p "Create $SMALL_JOB (8 GiB, parallelism 4)? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    if gcloud run jobs describe "$SMALL_JOB" --region="$REGION" >/dev/null 2>&1; then
        log "$SMALL_JOB already exists; deploy.yml will update its image. Skipping create."
    else
        log "Creating $SMALL_JOB..."
        gcloud run jobs create "$SMALL_JOB" \
            --image="$EXISTING_IMAGE" \
            --region="$REGION" \
            --service-account="$JOB_RUNNER_SA" \
            --memory=8Gi \
            --cpu=2 \
            --parallelism=4 \
            --task-timeout=3600 \
            --set-env-vars="MODE=sa_sb_worker,SA_SB_WORKER_MODE=small"
        log "  created. Re-run github actions deploy to populate env vars from secrets."
    fi
fi

read -r -p "Create $BIG_JOB (32 GiB, parallelism 1)? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    if gcloud run jobs describe "$BIG_JOB" --region="$REGION" >/dev/null 2>&1; then
        log "$BIG_JOB already exists; deploy.yml will update its image. Skipping create."
    else
        log "Creating $BIG_JOB..."
        gcloud run jobs create "$BIG_JOB" \
            --image="$EXISTING_IMAGE" \
            --region="$REGION" \
            --service-account="$JOB_RUNNER_SA" \
            --memory=32Gi \
            --cpu=8 \
            --parallelism=1 \
            --task-timeout=7200 \
            --set-env-vars="MODE=sa_sb_worker,SA_SB_WORKER_MODE=big"
        log "  created. Re-run github actions deploy to populate env vars from secrets."
    fi
fi

# -------- Cloud Scheduler triggers --------
log ""
log "Granting runtime SA roles/run.invoker on the two new jobs (for Scheduler)..."
for job in "$SMALL_JOB" "$BIG_JOB"; do
    if gcloud run jobs describe "$job" --region="$REGION" >/dev/null 2>&1; then
        gcloud run jobs add-iam-policy-binding "$job" \
            --region="$REGION" \
            --member="serviceAccount:$JOB_RUNNER_SA" \
            --role="roles/run.invoker" >/dev/null
        log "  ✓ $job invokable by $JOB_RUNNER_SA"
    fi
done

JOB_URI_BASE="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs"

create_scheduler() {
    local trigger_name="$1"
    local job_name="$2"
    local schedule="$3"
    if gcloud scheduler jobs describe "$trigger_name" --location="$REGION" >/dev/null 2>&1; then
        log "  $trigger_name exists; updating schedule..."
        gcloud scheduler jobs update http "$trigger_name" \
            --location="$REGION" \
            --schedule="$schedule" >/dev/null
    else
        log "  Creating $trigger_name (cron='$schedule', target=$job_name)..."
        gcloud scheduler jobs create http "$trigger_name" \
            --location="$REGION" \
            --schedule="$schedule" \
            --http-method=POST \
            --uri="${JOB_URI_BASE}/${job_name}:run" \
            --oauth-service-account-email="$JOB_RUNNER_SA" \
            --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
    fi
}

read -r -p "Create / update Cloud Scheduler triggers? [y/N] " ans
if [[ "$ans" =~ ^[Yy]$ ]]; then
    create_scheduler "$SMALL_TRIGGER" "$SMALL_JOB" "$SMALL_SCHEDULE"
    create_scheduler "$BIG_TRIGGER" "$BIG_JOB" "$BIG_SCHEDULE"
fi

log ""
log "Done. Next steps:"
log "  1. Push the feature branch and let GitHub Actions deploy a fresh"
log "     image with all env vars."
log "  2. Manually trigger one run to verify:"
log "       gcloud run jobs execute $SMALL_JOB --region=$REGION --wait"
log "  3. Watch the /config page for the SA/SB worker run summary."
