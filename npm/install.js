const fs = require('fs');
const path = require('path');
const https = require('https');
const { execSync } = require('child_process');

// Configuration
const VERSION = 'v0.0.1'; // Update this to match the tag you want to download
const REPO = 'shivang-16/mole_db';
const BIN_NAME = 'mole-cli';

// Detect OS and Arch
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

const binaryName = `${BIN_NAME}-${platform}-${arch}${platform === 'windows' ? '.exe' : ''}`;
const downloadUrl = `https://github.com/${REPO}/releases/download/${VERSION}/${binaryName}`;
const dest = path.join(__dirname, 'bin', platform === 'windows' ? 'mole.exe' : 'mole');

console.log(`Downloading mole-cli ${VERSION} for ${platform}/${arch}...`);

if (!fs.existsSync(path.join(__dirname, 'bin'))) {
    fs.mkdirSync(path.join(__dirname, 'bin'));
}

const file = fs.createWriteStream(dest);

https.get(downloadUrl, (response) => {
    if (response.statusCode === 302) {
        // Handle redirect
        https.get(response.headers.location, (response) => {
            response.pipe(file);
            file.on('finish', () => {
                file.close();
                if (platform !== 'windows') {
                    fs.chmodSync(dest, 0o755); // Make executable
                }
                console.log('Download complete!');
            });
        });
    } else if (response.statusCode !== 200) {
        console.error(`Failed to download binary: HTTP ${response.statusCode}`);
        console.error(downloadUrl);
        process.exit(1);
    } else {
        response.pipe(file);
        file.on('finish', () => {
             file.close();
             if (platform !== 'windows') {
                 fs.chmodSync(dest, 0o755);
             }
             console.log('Download complete!');
        });
    }
}).on('error', (err) => {
    fs.unlink(dest, () => {});
    console.error(`Error downloading file: ${err.message}`);
    process.exit(1);
});
