const puppeteer = require('puppeteer');
const path = require('path');

(async () => {
    console.log('Launching browser...');
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();
    
    page.on('console', msg => {
        console.log(`[Browser Console] ${msg.type().toUpperCase()}: ${msg.text()}`);
    });
    
    page.on('pageerror', err => {
        console.error(`[Browser PageError] ${err.toString()}`);
    });
    
    console.log('Navigating to live dashboard...');
    await page.goto('https://whatsappbot-production-d81c.up.railway.app/', { waitUntil: 'networkidle2' });
    
    console.log('Taking screenshot...');
    await page.screenshot({ path: path.join(__dirname, 'live_test.png') });
    
    console.log('Checking switchTab type...');
    const switchTabType = await page.evaluate(() => typeof switchTab);
    console.log(`switchTab type: ${switchTabType}`);
    
    console.log('Clicking Instagram Bot tab...');
    try {
        await page.evaluate(() => {
            const btn = document.getElementById('tab-insta-btn');
            if (btn) btn.click();
            else console.error('tab-insta-btn not found!');
        });
        
        console.log('Waiting 2 seconds...');
        await new Promise(r => setTimeout(r, 2000));
        
        const activePanel = await page.evaluate(() => {
            const panels = document.querySelectorAll('.tab-panel');
            let active = null;
            panels.forEach(p => {
                if (p.classList.contains('active')) active = p.id;
            });
            return active;
        });
        console.log(`Active panel after click: ${activePanel}`);
        
        await page.screenshot({ path: path.join(__dirname, 'live_test_after_click.png') });
    } catch (e) {
        console.error('Click failed:', e);
    }
    
    await browser.close();
    console.log('Done!');
})();
