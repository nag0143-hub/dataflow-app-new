export function toYaml(obj, indent = 0) {
  const pad = "  ".repeat(indent);
  if (obj === null || obj === undefined) return "null";
  if (typeof obj === "boolean") return obj ? "true" : "false";
  if (typeof obj === "number") return String(obj);
  if (typeof obj === "string") {
    if (/[\n:#\[\]{},'"&*?|<>=!%@`]/.test(obj) || obj === "" || /^(true|false|null|yes|no)$/i.test(obj)) {
      return `"${obj.replace(/"/g, '\\"')}"`;
    }
    return obj;
  }
  if (Array.isArray(obj)) {
    if (obj.length === 0) return "[]";
    return obj.map(item => {
      const val = toYaml(item, indent + 1);
      if (typeof item === "object" && item !== null && !Array.isArray(item)) {
        const lines = val.split("\n");
        return `${pad}- ${lines[0]}\n${lines.slice(1).join("\n")}`;
      }
      return `${pad}- ${val}`;
    }).join("\n");
  }
  if (typeof obj === "object") {
    const keys = Object.keys(obj);
    if (keys.length === 0) return "{}";
    return keys.map(k => {
      const val = obj[k];
      if (val !== null && typeof val === "object" && !Array.isArray(val)) {
        const nested = toYaml(val, indent + 1);
        if (nested === "{}") return `${pad}${k}: {}`;
        return `${pad}${k}:\n${nested}`;
      }
      if (Array.isArray(val)) {
        if (val.length === 0) return `${pad}${k}: []`;
        if (typeof val[0] === "object") return `${pad}${k}:\n${toYaml(val, indent + 1)}`;
      }
      return `${pad}${k}: ${toYaml(val, indent)}`;
    }).join("\n");
  }
  return String(obj);
}
