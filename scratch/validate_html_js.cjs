const fs = require('fs');
const path = require('path');
const vm = require('vm');

const htmlPath = path.join(__dirname, '..', 'public', 'index.html');
const html = fs.readFileSync(htmlPath, 'utf8');

const regex = /<script\b[^>]*>([\s\S]*?)<\/script>/gi;
let match;
let count = 0;

while ((match = regex.exec(html)) !== null) {
    const js = match[1];
    if (!js.trim()) continue;
    count++;
    console.log(`Compiling Script Block #${count}...`);
    try {
        new vm.Script(js, { filename: `index.html#script-${count}` });
        console.log(`Script Block #${count} compiled successfully!`);
    } catch (e) {
        console.error(`Error in Script Block #${count}:`, e);
    }
}
