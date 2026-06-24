import makeWASocket, { useMultiFileAuthState } from '@whiskeysockets/baileys';
import pino from 'pino';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function test() {
    console.log("Checking exports of @whiskeysockets/baileys...");
    try {
        const pkg = await import('@whiskeysockets/baileys');
        console.log("Exported keys:", Object.keys(pkg).filter(k => k.toLowerCase().includes('url') || k.toLowerCase().includes('preview')));
        
        // Let's check if getUrlInfo exists on the default or named exports
        if (pkg.getUrlInfo) {
            console.log("getUrlInfo exported directly!");
        } else if (pkg.default && pkg.default.getUrlInfo) {
            console.log("getUrlInfo exported under default!");
        } else {
            console.log("getUrlInfo not in main exports, trying subpaths...");
            try {
                const linkPreviewPkg = await import('@whiskeysockets/baileys/lib/Utils/link-preview.js');
                console.log("Subpath link-preview exports:", Object.keys(linkPreviewPkg));
            } catch (err) {
                console.log("Subpath link-preview import failed:", err.message);
            }
        }
    } catch (e) {
        console.error("Test failed:", e);
    }
}

test();
