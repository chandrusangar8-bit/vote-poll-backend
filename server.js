import { spawn } from 'node:child_process';

const child = spawn(process.execPath, ['--import', 'tsx/esm', 'server.ts'], {
	stdio: 'inherit',
	env: process.env
});

child.on('exit', (code, signal) => {
	process.exit(code ?? (signal ? 1 : 0));
});