export async function retry(fn, retries = 5, baseDelay = 300, sleep) {
    let lastError;
    for (let i = 0; i < retries; i++) {
        try {
            return await fn();
        } catch (err) {
            lastError = err;
            if (i === retries - 1) break;
            const delay = baseDelay * 2 ** i + Math.random() * 100;
            await sleep(delay);
        }
    }
    throw lastError;
}