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
    { name: 'mole', remote: `mole-${platform}-${arch}${ext}` },
    { name: 'mole-cli', remote: `mole-cli-${platform}-${arch}${ext}` }
];

console.log(`Installing Mole DB v${VERSION} for ${platform}/${arch}...`);

function downloadBinary(binaryInfo) {
    return new Promise((resolve, reject) => {
        const downloadUrl = `https://github.com/${REPO}/releases/download/v${VERSION}/${binaryInfo.remote}`;
        const dest = path.join(binDir, `${binaryInfo.name}${ext}`);

        console.log(`Downloading ${binaryInfo.name}...`);

        const file = fs.createWriteStream(dest);
        
        https.get(downloadUrl, (response) => {
            if (response.statusCode === 302 || response.statusCode === 301) {
                https.get(response.headers.location, (response) => {
                    response.pipe(file);
                    file.on('finish', () => {
                        file.close();
                        if (!isWindows) {
                            fs.chmodSync(dest, 0o755);
                        }
                        console.log(`✓ ${binaryInfo.name} installed`);
                        resolve();
                    });
                }).on('error', reject);
            } else if (response.statusCode !== 200) {
                fs.unlink(dest, () => {});
                reject(new Error(`Failed to download ${binaryInfo.name}: HTTP ${response.statusCode}`));
            } else {
                response.pipe(file);
                file.on('finish', () => {
                    file.close();
                    if (!isWindows) {
                        fs.chmodSync(dest, 0o755);
                    }
                    console.log(`✓ ${binaryInfo.name} installed`);
                    resolve();
                });
            }
        }).on('error', (err) => {
            fs.unlink(dest, () => {});
            reject(err);
        });
    });
}

Promise.all(binaries.map(downloadBinary))
    .then(() => {
        console.log('\n✨ Mole DB installed successfully!');
        console.log('\nUsage:');
        console.log('  mole              # Start the server');
        console.log('  mole-cli          # Connect to the server');
    })
    .catch((err) => {
        console.error(`\nInstallation failed: ${err.message}`);
        console.error(`\nPlease try installing manually from:`);
        console.error(`https://github.com/${REPO}/releases/tag/v${VERSION}`);
        process.exit(1);
    });
