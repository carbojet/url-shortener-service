/**
 * URL shortener Service (Node.js)
 * * Handles POST requests to shorten URLs, integrates Base62 encoding, and uses UUIDs.
 */
const http = require("http");
const { Pool } = require("pg");
const Redis = require("ioredis");
// We need a library to generate UUIDs
const { v4: uuidv4 } = require("uuid"); // Not strictly used, but good practice if needed later

// --- 1. Database Configuration and Initialization ---

// PostgreSQL Pool for Durable Storage
const pgPool = new Pool({
    user: process.env.POSTGRES_USER || "postgres",
    host: "postgres",
    database: process.env.POSTGRES_DB || "urlshortener",
    password: process.env.POSTGRES_PASSWORD || "postgres",
    port: 5432,
    connectionTimeoutMillis: 5000,
    idleTimeoutMillis: 30000,
});

// Redis Client for High-Speed Cache/Lookup
const redisClient = new Redis({
    host: "redis",
    port: 6379,
});

redisClient.on("connect", () => {
    console.log("[REDIS] Successfully connected to Redis.");
});
redisClient.on("error", (err) => {
    console.error("[REDIS] Connection error:", err.message);
});

// --- 2. Key Generation & Encoding (No Change) ---
const ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
const BASE = ALPHABET.length; // 62
const SHORT_URL_PREFIX = "http://s.ly/";
let currentEncodingId = 10000000;

function getEncodingId() {
    currentEncodingId += Math.floor(Math.random() * 5) + 1;
    return currentEncodingId;
}

function encode(num) {
    if (num === 0) return ALPHABET[0];
    let encoded = "";
    while (num > 0) {
        encoded = ALPHABET[num % BASE] + encoded;
        num = Math.floor(num / BASE);
    }
    return encoded;
}

// --- 3. Persistence (Dual Write) Logic ---

async function saveMapping(shortKey, longUrl, expiresAt) {
    const expiresAtISO = expiresAt.toISOString();

    // 1. Write to PostgreSQL (Durable Storage)
    const pgQuery = `
        INSERT INTO url_mappings(short_key, long_url, expires_at)
        VALUES($1, $2, $3)
        RETURNING id;
    `;
    
    // We expect the connection to be available now due to the init chain
    const result = await pgPool.query(pgQuery, [shortKey, longUrl, expiresAtISO]);
    const id = result.rows[0].id;
    console.log(
        `[PG DUAL WRITE] Durable record created with UUID: ${id} for key: ${shortKey}`
    );

    // 2. Write to Redis (High-Speed Read Path Cache)
    const expirationSeconds = Math.floor(
        (expiresAt.getTime() - Date.now()) / 1000
    );
    await redisClient.set(shortKey, longUrl, "EX", expirationSeconds);
    console.log(
        `[REDIS DUAL WRITE] Cache set for key: ${shortKey} with TTL: ${expirationSeconds}s`
    );

    return {
        id,
        short_key: shortKey,
        long_url: longUrl,
        expires_at: expiresAtISO,
    };
}


// --- 4. HTTP Server Implementation (URL shortener Service) ---
function startServer() {
    const server = http.createServer(async (req, res) => {
        res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        res.setHeader("Access-Control-Allow-Headers", "Content-Type");

        if (req.method === "OPTIONS") {
            res.writeHead(204);
            res.end();
            return;
        }

        // Health Check 
        if (req.method === "GET" && req.url === "/") {
            res.writeHead(200, { "Content-Type": "text/plain" });
            res.end("URL shortener Service is healthy and ready!");
            return;
        }

        if (req.method === "POST" && req.url === "/api/v1/shorten") {
            let body = "";

            req.on("data", (chunk) => {
                body += chunk.toString();
            });

            req.on("end", async () => {
                try {
                    if (!body) {
                        res.writeHead(400, { "Content-Type": "application/json" });
                        res.end(JSON.stringify({ error: "Request body cannot be empty." }));
                        return;
                    }

                    const { long_url: longUrl, custom_key: customKey } = JSON.parse(body);

                    if (!longUrl) {
                        res.writeHead(400, { "Content-Type": "application/json" });
                        res.end(
                            JSON.stringify({ error: "Missing long_url in request body." })
                        );
                        return;
                    }

                    const expiresAt = new Date(Date.now() + 365 * 24 * 60 * 60 * 1000);

                    let shortKey;

                    if (customKey) {
                        // Check Redis for custom key existence (fast pre-check)
                        const exists = await redisClient.exists(customKey);
                        if (exists) {
                            res.writeHead(409, { "Content-Type": "application/json" });
                            res.end(
                                JSON.stringify({
                                    error: `Custom key '${customKey}' is already taken.`,
                                })
                            );
                            return;
                        }
                        shortKey = customKey;
                    } else {
                        // Generate new key from sequential number
                        const encodingId = getEncodingId();
                        console.log(`[KGS MOCK] Acquired encoding ID: ${encodingId}`);
                        shortKey = encode(encodingId);
                        console.log(`[BASE62] Encoded ID ${encodingId} to Key: ${shortKey}`);
                    }

                    // Persistence (Dual Write to PG and Redis, PG generates the UUID)
                    const record = await saveMapping(shortKey, longUrl, expiresAt);

                    // Response to Client
                    res.writeHead(201, { "Content-Type": "application/json" });
                    res.end(
                        JSON.stringify({
                            id: record.id,
                            short_url: `${SHORT_URL_PREFIX}${shortKey}`,
                            long_url: longUrl,
                            key: shortKey,
                            expires_at: record.expires_at,
                            message: customKey
                                ? "Custom link created."
                                : "Link created successfully.",
                        })
                    );
                } catch (error) {
                    console.error("Processing error:", error.stack);
                    // Check specifically for PG unique constraint violation (for custom keys)
                    if (error.code === '23505') { 
                        res.writeHead(409, { "Content-Type": "application/json" });
                        res.end(
                            JSON.stringify({
                                error: "Key conflict: Generated key already exists. Please retry the request.",
                            })
                        );
                    } else {
                        res.writeHead(500, { "Content-Type": "application/json" });
                        res.end(
                            JSON.stringify({
                                error:
                                    "Internal server error during request processing. Check logs for database connection issues.",
                            })
                        );
                    }
                }
            });
        } else {
            res.writeHead(404, { "Content-Type": "text/plain" });
            res.end("Not Found. Use GET / or POST /api/v1/shorten");
        }
    });

    const PORT = 3000;
    server.listen(PORT, () => {
        console.log(`\nURL shortener Service is now listening on port ${PORT}`);
    });
}

// --- 5. Initialization Chain (Retry and Server Start Logic) ---

// Helper function to wait for a delay
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Recursive function to retry connection and database setup
async function connectWithRetry(retries = 10) {
    let client;
    try {
        console.log(`[PG INIT] Attempting connection to PostgreSQL... (Attempts left: ${retries})`);
        client = await pgPool.connect();
        
        // 1. Enable UUID support
        await client.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);
        
        // 2. Create Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS url_mappings (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), 
                short_key VARCHAR(10) UNIQUE NOT NULL,
                long_url TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP WITH TIME ZONE
            );
        `);
        
        console.log("[PG] Verified/Created url_mappings table with UUID primary key. READY.");
        client.release();
        return true; // Success
    } catch (err) {
        if (client) {
            client.release(); // Ensure connection is released on error
        }

        // Only retry if the error is ECONNREFUSED (network/timing issue) and retries remain
        if (err.code === 'ECONNREFUSED' && retries > 0) {
            const delay = 2000; // 2 seconds delay
            console.warn(`[PG INIT] Connection refused. Retrying in ${delay / 1000}s...`);
            await sleep(delay);
            return connectWithRetry(retries - 1); // Recurse with one fewer retry
        }

        console.error("[PG] FATAL: Initial database connection or setup failed. Shutting down:", err.stack);
        return false; // Fatal error
    }
}

// Start the whole initialization process
connectWithRetry()
    .then((success) => {
        if (success) {
            // Start the server only after DB init is confirmed
            startServer();
        } else {
            // Exit if a fatal, unrecoverable DB error occurred
            console.error("Unrecoverable database error. Exiting process.");
            // process.exit(1); // Would exit the container in a real environment
        }
    });
