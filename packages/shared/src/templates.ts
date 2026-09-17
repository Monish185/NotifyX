/**
 * Safe, deterministic templating engine for NotifyX.
 * Supports mustache-style {{variable.path}} interpolation without eval or arbitrary JS.
 */

export const MAX_TEMPLATE_LENGTH = 65536; // 64 KB
export const MAX_VARIABLE_VALUE_LENGTH = 10240; // 10 KB

/**
 * Extracts all unique {{variable.path}} references from a template string.
 */
export function extractTemplateVariables(content: string): string[] {
  if (!content) return [];
  const regex = /\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}/g;
  const variables = new Set<string>();
  let match: RegExpExecArray | null;
  while ((match = regex.exec(content)) !== null) {
    if (match[1]) {
      variables.add(match[1]);
    }
  }
  return Array.from(variables);
}

/**
 * Resolves a dotted path (e.g. "user.profile.name") against an arbitrary object.
 */
export function getNestedValue(obj: unknown, path: string): unknown {
  if (obj === null || obj === undefined || typeof obj !== 'object') {
    return undefined;
  }
  // If direct property exists (e.g. obj['user.name']), prefer it
  if (Object.prototype.hasOwnProperty.call(obj, path)) {
    return (obj as any)[path];
  }
  const parts = path.split('.');
  let current: any = obj;
  for (const part of parts) {
    if (current === null || current === undefined || typeof current !== 'object') {
      return undefined;
    }
    current = current[part];
  }
  return current;
}

/**
 * Validates that all required variable paths are non-null and non-undefined in data.
 */
export function validateTemplateVariables(
  requiredVariables: string[],
  data: Record<string, unknown>
): { valid: boolean; missing: string[] } {
  const missing: string[] = [];
  for (const varPath of requiredVariables) {
    const val = getNestedValue(data, varPath);
    if (val === undefined || val === null) {
      missing.push(varPath);
    }
  }
  return {
    valid: missing.length === 0,
    missing,
  };
}

/**
 * Safely escapes HTML special characters.
 */
export function escapeHtml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

export interface RenderTemplateOptions {
  escape?: boolean;
}

/**
 * Deterministically renders a template string with the provided data dictionary.
 * Throws a descriptive error if any required variable is missing or if size limits are violated.
 */
export function renderTemplate(
  content: string,
  data: Record<string, unknown>,
  options: RenderTemplateOptions = {}
): string {
  if (!content) return '';
  if (content.length > MAX_TEMPLATE_LENGTH) {
    throw new Error(`Template exceeds maximum size of ${MAX_TEMPLATE_LENGTH} bytes`);
  }

  return content.replace(/\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}/g, (_match, path) => {
    const value = getNestedValue(data, path);
    if (value === undefined || value === null) {
      throw new Error(`Missing variable required for template rendering: "${path}"`);
    }

    let stringVal: string;
    if (typeof value === 'object') {
      stringVal = JSON.stringify(value);
    } else {
      stringVal = String(value);
    }

    if (stringVal.length > MAX_VARIABLE_VALUE_LENGTH) {
      throw new Error(`Variable "${path}" exceeds maximum value length of ${MAX_VARIABLE_VALUE_LENGTH} characters`);
    }

    return options.escape ? escapeHtml(stringVal) : stringVal;
  });
}
