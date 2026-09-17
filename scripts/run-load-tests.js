/**
 * High-level orchestration script for Phase 8 Load Testing.
 * Invokes the standalone @notifyx/load-test tooling.
 */
import { spawn } from 'node:child_process';
import path from 'node:path';

const args = process.argv.slice(2);
const scenario = args.find((a) => a.startsWith('--scenario=')) || '--scenario=all';
const rps = args.find((a) => a.startsWith('--rps=')) || '--rps=20';
const duration = args.find((a) => a.startsWith('--duration=')) || '--duration=30';

console.log('================================================================');
console.log('       NotifyX Phase 8: Load Testing Runner                     ');
console.log(`       Args: ${scenario} ${rps} ${duration}                    `);
console.log('================================================================\n');

const tsxPath = path.resolve('node_modules', '.bin', process.platform === 'win32' ? 'tsx.cmd' : 'tsx');
const toolEntry = path.resolve('tools', 'load-test', 'src', 'index.ts');

const child = spawn(tsxPath, [toolEntry, scenario, rps, duration, ...args.filter(a => !a.startsWith('--scenario=') && !a.startsWith('--rps=') && !a.startsWith('--duration='))], {
  stdio: 'inherit',
  shell: true,
  env: process.env,
});

child.on('close', (code) => {
  if (code !== 0) {
    console.error(`Load test process exited with code ${code}`);
    process.exit(code || 1);
  }
  console.log('\n[SUCCESS] Phase 8 Load Test execution finished successfully.');
});
