/**
 * CSV utilities for column mappings import/export
 */

const CSV_HEADERS = [
  'source',
  'sourceDataType',
  'sourceLength',
  'target',
  'targetDataType',
  'targetLength',
  'transformation',
  'targetPosition'
];

/**
 * Export column mappings to CSV
 * @param {Array} mappings - Array of mapping objects
 * @param {string} tableName - Name of the table for filename
 */
export function exportMappingsToCSV(mappings, tableName = 'mappings') {
  const nonAuditMappings = mappings.filter(m => !m.is_audit);
  
  if (nonAuditMappings.length === 0) {
    alert('No mappings to export');
    return;
  }

  // Create CSV header
  const header = CSV_HEADERS.join(',');
  
  // Create CSV rows
  const rows = nonAuditMappings.map(mapping => {
    return CSV_HEADERS.map(field => {
      const value = mapping[field] || '';
      // Escape quotes and wrap in quotes if contains comma or newline
      const stringValue = String(value);
      if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
        return `"${stringValue.replace(/"/g, '""')}"`;
      }
      return stringValue;
    }).join(',');
  });

  const csv = [header, ...rows].join('\n');
  
  // Download file
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');
  const url = URL.createObjectURL(blob);
  link.setAttribute('href', url);
  link.setAttribute('download', `${tableName}-mappings-${new Date().toISOString().split('T')[0]}.csv`);
  link.style.visibility = 'hidden';
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

/**
 * Import mappings from CSV file
 * @param {File} file - CSV file to import
 * @returns {Promise<Array>} Array of mapping objects
 */
export function importMappingsFromCSV(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    
    reader.onload = (e) => {
      try {
        const csv = e.target.result;
        const lines = csv.split('\n').filter(line => line.trim());
        
        if (lines.length < 2) {
          reject(new Error('CSV must contain header and at least one mapping'));
          return;
        }

        const headers = parseCSVLine(lines[0]);
        
        // Validate headers
        const requiredHeaders = ['source', 'target', 'transformation'];
        const missingHeaders = requiredHeaders.filter(h => !headers.includes(h));
        if (missingHeaders.length > 0) {
          reject(new Error(`Missing required columns: ${missingHeaders.join(', ')}`));
          return;
        }

        const mappings = [];
        for (let i = 1; i < lines.length; i++) {
          const values = parseCSVLine(lines[i]);
          if (values.length === 0) continue;
          
          const mapping = {};
          headers.forEach((header, idx) => {
            if (values[idx] !== undefined && values[idx] !== '') {
              mapping[header] = values[idx];
            }
          });

          if (mapping.source && mapping.target && mapping.transformation) {
            mappings.push(mapping);
          }
        }

        if (mappings.length === 0) {
          reject(new Error('No valid mappings found in CSV'));
          return;
        }

        resolve(mappings);
      } catch (error) {
        reject(new Error(`Error parsing CSV: ${error.message}`));
      }
    };

    reader.onerror = () => {
      reject(new Error('Failed to read file'));
    };

    reader.readAsText(file);
  });
}

/**
 * Parse a single CSV line handling quoted fields
 * @param {string} line - CSV line to parse
 * @returns {Array<string>} Array of parsed values
 */
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let insideQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const nextChar = line[i + 1];

    if (char === '"') {
      if (insideQuotes && nextChar === '"') {
        current += '"';
        i++;
      } else {
        insideQuotes = !insideQuotes;
      }
    } else if (char === ',' && !insideQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }

  result.push(current.trim());
  return result;
}