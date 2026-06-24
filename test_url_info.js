import { getUrlInfo } from '@whiskeysockets/baileys';

async function test() {
    const url = 'https://www.radikikk.shop/products/seznik-portable-a4-printer-bluetooth-inkless-thermal-printer-usb-bluetooth-compatible-with-android-ios-laptop-1-year-warranty-lite-203dpi-black?_pos=1&_sid=e81c8c839&_ss=r';
    console.log("Fetching URL metadata for:", url);
    try {
        const info = await getUrlInfo(url);
        console.log("URL Metadata result:", info);
    } catch (e) {
        console.error("Failed to get URL metadata:", e);
    }
}

test();
