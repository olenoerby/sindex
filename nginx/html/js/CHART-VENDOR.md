Download a Chart.js build and save it as `chart.min.js` in this folder so the analytics page loads it from the same origin.

Recommended (example):

- Use a stable Chart.js build (v4.x+).

Linux/macOS:

curl -L -o chart.min.js https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js

Windows PowerShell:

Invoke-WebRequest -Uri "https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js" -OutFile chart.min.js

After placing `chart.min.js` here, reload `analytics.html`. Hosting it locally prevents browser tracking-protection from treating Chart.js as a third-party storage-accessor and avoids the console warning.