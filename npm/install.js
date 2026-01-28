const fs = require('fs');
const path = require('path');
const https = require('https');

const VERSION = require('./package.json').version;
const REPO = 'shivang-16/mole_db';

const osMap = {
    'darwin': 'darwin',
    'linux': 'linux',
    'win32': 'windows'
};

const archMap = {
    'x64': 'amd64',
    'arm64': 'arm64'
};

const platform = osMap[process.platform];
const arch = archMap[process.arch];

if (!platform || !arch) {
    console.error(`Unsupported platform: ${process.platform} ${process.arch}`);
    process.exit(1);
}

const binDir = path.join(__dirname, 'bin');
if (!fs.existsSync(binDir)) {
    fs.mkdirSync(binDir, { recursive: true });
}

const isWindows = platform === 'windows';
const ext = isWindows ? '.exe' : '';

const binaries = [
    { name: 'mole', remote: `mole-${platform}-${arch}${ext}` }
];

console.log(`\n📦 Installing Mole DB v${VERSION} for ${platform}/${arch}...\n`);

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

function downloadBinary(binaryInfo) {
    return new Promise((resolve, reject) => {
        const downloadUrl = `https://github.com/${REPO}/releases/download/v${VERSION}/${binaryInfo.remote}`;
        const dest = path.join(binDir, `${binaryInfo.name}${ext}`);

        process.stdout.write(`⬇️  Downloading ${binaryInfo.name}... `);

        const file = fs.createWriteStream(dest);
        let downloadedBytes = 0;
        let totalBytes = 0;
        let lastUpdate = Date.now();
        
        const handleResponse = (response) => {
            totalBytes = parseInt(response.headers['content-length'] || '0', 10);
            
            response.on('data', (chunk) => {
                downloadedBytes += chunk.length;
                const now = Date.now();
                if (now - lastUpdate > 500) {
                    const percent = totalBytes > 0 ? Math.round((downloadedBytes / totalBytes) * 100) : 0;
                    const downloaded = formatBytes(downloadedBytes);
                    const total = totalBytes > 0 ? formatBytes(totalBytes) : '?';
                    process.stdout.write(`\r⬇️  Downloading ${binaryInfo.name}... ${percent}% (${downloaded}/${total})`);
                    lastUpdate = now;
                }
            });

            response.pipe(file);
            file.on('finish', () => {
                file.close();
                if (!isWindows) {
                    fs.chmodSync(dest, 0o755);
                }
                const size = formatBytes(downloadedBytes);
                console.log(`\r✅ ${binaryInfo.name} installed (${size})              `);
                resolve();
            });
        };
        
        https.get(downloadUrl, (response) => {
            if (response.statusCode === 302 || response.statusCode === 301) {
                https.get(response.headers.location, handleResponse).on('error', reject);
            } else if (response.statusCode !== 200) {
                console.log(`\r❌ Failed to download ${binaryInfo.name}              `);
                fs.unlink(dest, () => {});
                reject(new Error(`Failed to download ${binaryInfo.name}: HTTP ${response.statusCode}`));
            } else {
                handleResponse(response);
            }
        }).on('error', (err) => {
            console.log(`\r❌ Download error              `);
            fs.unlink(dest, () => {});
            reject(err);
        });
    });
}

Promise.all(binaries.map(downloadBinary))
    .then(() => {
        console.log('\n✨ Mole DB installed successfully!\n');
        console.log('Usage:');
        console.log('  mole                  # Start interactive mode (server + client)');
        console.log('  mole server -d        # Start server daemon');
        console.log('  mole cli              # Connect to running server');
        console.log('  mole help             # Show help\n');
    })
    .catch((err) => {
        console.error(`\n❌ Installation failed: ${err.message}\n`);
        console.error('Please try installing manually from:');
        console.error(`https://github.com/${REPO}/releases/tag/v${VERSION}\n`);
        process.exit(1);
    });
