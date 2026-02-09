# Railway Deployment Guide

This guide covers deploying the Company Email Scraper to Railway.

## Prerequisites

- Railway account
- GitHub repository with the code

## Step 1: Create PostgreSQL Database

1. In Railway dashboard, click "New Project"
2. Select "Provision PostgreSQL"
3. Railway will automatically create the database and provide `DATABASE_URL`

## Step 2: Create Application Service

1. In the same project, click "New" → "GitHub Repo"
2. Select your repository
3. Set the root directory to `company_email_scraper`
4. Railway will build using the project's `Dockerfile` (explicitly set via `railway.json`)

## Step 3: Configure Environment Variables

Set the following in Railway dashboard (Settings → Variables):

| Variable | Required | Description |
|----------|----------|-------------|
| `JWT_SECRET_KEY` | Yes | Generate a secure random string (e.g., `openssl rand -hex 32`) |
| `USERS` | Yes | Format: `email1:password1,email2:password2` |
| `MANUS_API_KEY` | Yes | Your Manus API key |
| `GEMINI_API_KEY` | Yes | Your Google Gemini API key |
| `SMTP_PASSWORD` | Yes* | Your SendGrid API key (*if EMAIL_ENABLED=true) |
| `FROM_EMAIL` | Yes | Your sender email address |
| `BASE_URL` | Yes | Your Railway app URL (e.g., `https://your-app.up.railway.app`) |
| `MANUS_WEBHOOK_URL` | Yes | `https://your-app.up.railway.app/webhooks/manus` |
| `COOKIE_SECURE` | No | Set to `true` (default in production) |
| `EMAIL_ENABLED` | No | Set to `true` (default) |

**Note:** `DATABASE_URL` and `PORT` are automatically set by Railway.

## Step 4: Link Database to Application

1. In Railway dashboard, click on the PostgreSQL service
2. Go to "Variables" tab
3. Click "Add Reference" on `DATABASE_URL`
4. Select your application service
5. Railway will automatically inject the connection string

## Step 5: Deploy

1. Push code to GitHub
2. Railway will automatically build and deploy
3. Monitor deployment logs for migration success
4. Verify health endpoint: `https://your-app.up.railway.app/health`

## Step 6: Configure Manus Webhook

1. In Manus dashboard, set webhook URL to: `https://your-app.up.railway.app/webhooks/manus`
2. Optionally set `MANUS_WEBHOOK_SECRET` in Railway for signature validation

## Step 7: Test Deployment

1. Access login page: `https://your-app.up.railway.app/`
2. Submit a test job with a small city
3. Monitor job progress in history page
4. Verify email notifications are sent
5. Download CSV results

## Environment Variables Summary

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | Yes | - | PostgreSQL connection (Railway auto-sets) |
| `PORT` | No | 8000 | Server port (Railway auto-sets) |
| `JWT_SECRET_KEY` | Yes | - | Secret for JWT token signing |
| `USERS` | Yes | - | User credentials (email:pass,email:pass) |
| `MANUS_API_KEY` | Yes | - | Manus API authentication |
| `GEMINI_API_KEY` | Yes | - | Gemini API authentication |
| `SMTP_PASSWORD` | Yes* | - | SendGrid API key (*if EMAIL_ENABLED=true) |
| `FROM_EMAIL` | Yes | noreply@yourapp.com | Sender email address |
| `BASE_URL` | Yes | http://localhost:8000 | Application base URL |
| `MANUS_WEBHOOK_URL` | Yes | - | Webhook endpoint for Manus |
| `COOKIE_SECURE` | No | true | Use secure cookies (must be true in prod) |
| `EMAIL_ENABLED` | No | true | Enable email notifications |
| `JOB_TIMEOUT_HOURS` | No | 12 | Job timeout threshold |

## Monitoring

- Railway provides automatic health checks via `/health` endpoint
- View logs in Railway dashboard
- Monitor database connections and performance
- Set up alerts for failed deployments

## Troubleshooting

### Migrations fail
- Check `DATABASE_URL` is correctly set and linked
- Verify PostgreSQL service is running

### Static files don't load
- Verify the application is using relative paths (Path(__file__).parent)
- Check build logs for any missing files

### Webhooks fail
- Check `MANUS_WEBHOOK_URL` matches your Railway domain exactly
- Verify the webhook endpoint is accessible: `curl https://your-app.up.railway.app/webhooks/manus`

### Emails fail
- Verify SMTP credentials are correct
- Ensure `EMAIL_ENABLED=true` is set
- Check SendGrid account is active and verified

### Jobs stuck
- Check Manus API key is valid
- Verify webhook configuration in Manus dashboard
- Monitor job timeout settings (`JOB_TIMEOUT_HOURS`)

## Graceful Shutdown

The application handles Railway's SIGTERM signal gracefully:
1. Stops accepting new requests
2. Waits up to 5 minutes for active jobs to complete their current phase
3. Closes HTTP clients and database connections
4. Jobs interrupted mid-phase will resume automatically on next startup

## Local Testing

Before deploying, test the startup script locally:

```bash
# Copy .env.example to .env and configure
cp .env.example .env

# Run the startup script
./startup.sh
```

Test graceful shutdown:
```bash
# Start the application
./startup.sh &

# Submit a job, then send SIGTERM
kill -TERM $!

# Check logs show "Waiting for active job tasks to complete"
```
