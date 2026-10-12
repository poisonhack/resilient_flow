
**resilient-flow** is a lightweight Python helper library designed to make bulk asynchronous API requests and data processing simple, safe, and robust.

Here is what it does for developers in a nutshell:

* **Concurrency & Rate Limiting:** Automatically controls how many tasks run at the exact same time and caps requests per second so you never overwhelm downstream APIs or servers.
* **Automatic Retries:** Handles temporary network glitches, timeouts, or server errors seamlessly using exponential backoff and randomized jitter.
* **Fault Tolerance:** If a single item out of thousands fails, it won't crash your entire script. It safely catches the error and returns a clean dictionary showing which items succeeded and which failed.
* **Live Progress Tracking:** Includes a built-in, optional terminal progress bar so you can visually monitor large batch jobs in real time.

**In short:** It saves developers from writing dozens of repetitive `asyncio`, semaphore, and error-handling boilerplate lines, turning complex bulk processing into just a few clean lines of code.

## Installation

```bash
pip install resilient-flow
