// ============================================================================
// LAKEHOUSE ENGINE OPERATIONS GENERATOR - MAIN JAVASCRIPT
// ============================================================================
// This script manages the interactive UI for generating JSON configurations
// for Lakehouse Engine table and file manager operations.
// ============================================================================

// ============================================================================
// DOM ELEMENT REFERENCES
// ============================================================================
// Cache frequently accessed DOM elements for better performance

/** Tab navigation buttons for switching between table and file managers */
const tabButtons = document.querySelectorAll('.tab-button');

/** Tab content containers for table and file manager sections */
const tabContents = document.querySelectorAll('.tab-content');

/** Dropdown select for choosing table manager operations */
const tableOperationSelect = document.getElementById('table-operation-select');

/** Dropdown select for choosing file manager operations */
const fileOperationSelect = document.getElementById('file-operation-select');

/** Container for dynamically generated table operation parameter fields */
const tableDynamicFields = document.getElementById('table-dynamic-fields');

/** Container for dynamically generated file operation parameter fields */
const fileDynamicFields = document.getElementById('file-dynamic-fields');

/** Button to add the currently configured operation to the list */
const addOperationBtn = document.getElementById('add-operation');

/** Button to clear all operations from the list */
const clearOperationsBtn = document.getElementById('clear-operations');

/** Container displaying the list of added operations */
const operationsList = document.getElementById('operations-list');

/** Button to generate JSON configuration from operations list */
const generateBtn = document.getElementById('generate-json');

/** Button to copy generated JSON to clipboard */
const copyBtn = document.getElementById('copy-json');

/** Button to download generated JSON as a file */
const downloadBtn = document.getElementById('download-json');

/** Button to format the displayed JSON */
const formatBtn = document.getElementById('format-json');

/** Button to validate the generated JSON configuration */
const validateBtn = document.getElementById('validate-json');

/** Pre-formatted text area displaying the generated JSON output */
const jsonOutput = document.getElementById('json-output');

/** Element displaying validation results and messages */
const validationResult = document.getElementById('validation-result');

/** Loading spinner overlay element */
const loading = document.getElementById('loading');

/** Toast notification element for user feedback */
const toast = document.getElementById('toast');

// ============================================================================
// APPLICATION STATE
// ============================================================================
// Global state variables that track the application's current status

/** Current active tab ('table-manager' or 'file-manager') */
let currentTab = 'table-manager';

/** Array of operation objects added by the user */
let operations = [];

/** Generated JSON configuration object */
let generatedConfig = null;

// ============================================================================
// OPERATION DEFINITIONS - TABLE MANAGER
// ============================================================================
// Defines all available table manager operations with their parameters,
// validation rules, and UI presentation details

/**
 * Table Manager Operations Configuration
 * Each operation includes:
 * - name: Display name for the UI
 * - icon: FontAwesome icon class
 * - fields: Array of field definitions with type, validation, and help text
 */
const TABLE_OPERATIONS = {
    'compute_table_statistics': {
        name: 'Compute Table Statistics',
        icon: 'fas fa-chart-bar',
        fields: [
            { name: 'table_or_view', label: 'Table or View Name', type: 'text', required: true, help: 'Name of the table or view to compute statistics for' }
        ]
    },
    'create_table': {
        name: 'Create Table',
        icon: 'fas fa-plus-square',
        fields: [
            { name: 'path', label: 'SQL File Path', type: 'text', required: true, help: 'Path to the SQL file containing the CREATE TABLE statement' },
            { name: 'disable_dbfs_retry', label: 'Disable DBFS Retry', type: 'select', options: ['True', 'False'], default: 'False', help: 'Whether to disable DBFS retry mechanism' },
            { name: 'delimiter', label: 'SQL Delimiter', type: 'text', default: ';', help: 'Delimiter to separate SQL commands' },
            { name: 'advanced_parser', label: 'Advanced Parser', type: 'select', options: ['True', 'False'], default: 'False', help: 'Use advanced SQL parser' }
        ]
    },
    'create_tables': {
        name: 'Create Multiple Tables',
        icon: 'fas fa-layer-group',
        fields: [
            { name: 'path', label: 'SQL File Paths', type: 'textarea', required: true, help: 'Comma-separated paths to SQL files containing CREATE TABLE statements' },
            { name: 'disable_dbfs_retry', label: 'Disable DBFS Retry', type: 'select', options: ['True', 'False'], default: 'False', help: 'Whether to disable DBFS retry mechanism' },
            { name: 'delimiter', label: 'SQL Delimiter', type: 'text', default: ';', help: 'Delimiter to separate SQL commands' },
            { name: 'advanced_parser', label: 'Advanced Parser', type: 'select', options: ['True', 'False'], default: 'False', help: 'Use advanced SQL parser' }
        ]
    },
    'create_view': {
        name: 'Create View',
        icon: 'fas fa-eye',
        fields: [
            { name: 'path', label: 'SQL File Path', type: 'text', required: true, help: 'Path to the SQL file containing the CREATE VIEW statement' },
            { name: 'disable_dbfs_retry', label: 'Disable DBFS Retry', type: 'select', options: ['True', 'False'], default: 'False', help: 'Whether to disable DBFS retry mechanism' },
            { name: 'delimiter', label: 'SQL Delimiter', type: 'text', default: ';', help: 'Delimiter to separate SQL commands' },
            { name: 'advanced_parser', label: 'Advanced Parser', type: 'select', options: ['True', 'False'], default: 'False', help: 'Use advanced SQL parser' }
        ]
    },
    'drop_table': {
        name: 'Drop Table',
        icon: 'fas fa-trash-alt',
        fields: [
            { name: 'table_or_view', label: 'Table Name', type: 'text', required: true, help: 'Name of the table to drop' }
        ]
    },
    'drop_view': {
        name: 'Drop View',
        icon: 'fas fa-eye-slash',
        fields: [
            { name: 'table_or_view', label: 'View Name', type: 'text', required: true, help: 'Name of the view to drop' }
        ]
    },
    'execute_sql': {
        name: 'Execute SQL',
        icon: 'fas fa-code',
        fields: [
            { name: 'sql', label: 'SQL Commands', type: 'textarea', required: true, help: 'SQL commands to execute (separated by delimiter)' },
            { name: 'delimiter', label: 'SQL Delimiter', type: 'text', default: ';', help: 'Delimiter to separate SQL commands' },
            { name: 'advanced_parser', label: 'Advanced Parser', type: 'select', options: ['True', 'False'], default: 'False', help: 'Use advanced SQL parser' }
        ]
    },
    'truncate': {
        name: 'Truncate Table',
        icon: 'fas fa-cut',
        fields: [
            { name: 'table_or_view', label: 'Table Name', type: 'text', required: true, help: 'Name of the table to truncate' }
        ]
    },
    'vacuum': {
        name: 'Vacuum Table',
        icon: 'fas fa-broom',
        fields: [
            { name: 'table_or_view', label: 'Table Name', type: 'text', help: 'Name of the table to vacuum (leave empty to use path)' },
            { name: 'path', label: 'Table Path', type: 'text', help: 'Path to the Delta table location (use if table_or_view is empty)' },
            { name: 'vacuum_hours', label: 'Retention Hours', type: 'number', default: '168', help: 'Number of hours to retain old versions (default: 168 hours = 7 days)' }
        ]
    },
    'describe': {
        name: 'Describe Table',
        icon: 'fas fa-info-circle',
        fields: [
            { name: 'table_or_view', label: 'Table or View Name', type: 'text', required: true, help: 'Name of the table or view to describe' }
        ]
    },
    'optimize': {
        name: 'Optimize Table',
        icon: 'fas fa-tachometer-alt',
        fields: [
            { name: 'table_or_view', label: 'Table Name', type: 'text', help: 'Name of the table to optimize (leave empty to use path)' },
            { name: 'path', label: 'Table Path', type: 'text', help: 'Path to the Delta table location (use if table_or_view is empty)' },
            { name: 'where_clause', label: 'Where Clause', type: 'text', help: 'Optional WHERE clause to limit optimization scope' },
            { name: 'optimize_zorder_col_list', label: 'Z-Order Columns', type: 'text', help: 'Comma-separated list of columns for Z-ORDER optimization' }
        ]
    },
    'show_tbl_properties': {
        name: 'Show Table Properties',
        icon: 'fas fa-cogs',
        fields: [
            { name: 'table_or_view', label: 'Table or View Name', type: 'text', required: true, help: 'Name of the table or view to show properties for' }
        ]
    },
    'get_tbl_pk': {
        name: 'Get Table Primary Key',
        icon: 'fas fa-key',
        fields: [
            { name: 'table_or_view', label: 'Table Name', type: 'text', required: true, help: 'Name of the table to get primary key from' }
        ]
    },
    'repair_table': {
        name: 'Repair Table',
        icon: 'fas fa-wrench',
        fields: [
            { name: 'table_or_view', label: 'Table Name', type: 'text', required: true, help: 'Name of the table to repair' },
            { name: 'sync_metadata', label: 'Sync Metadata', type: 'select', options: ['True', 'False'], default: 'False', help: 'Whether to sync metadata during repair' }
        ]
    },
    'delete_where': {
        name: 'Delete Where',
        icon: 'fas fa-eraser',
        fields: [
            { name: 'table_or_view', label: 'Table Name', type: 'text', required: true, help: 'Name of the table to delete from' },
            { name: 'where_clause', label: 'Where Clause', type: 'text', required: true, help: 'WHERE condition for deletion (without WHERE keyword)' }
        ]
    }
};

// ============================================================================
// OPERATION DEFINITIONS - FILE MANAGER
// ============================================================================
// Defines all available file manager operations for S3 and DBFS file systems

/**
 * File Manager Operations Configuration
 * Supports operations for:
 * - S3: delete, copy, move, restore from Glacier
 * - DBFS: delete, copy, move
 */
const FILE_OPERATIONS = {
    'delete_objects': {
        name: 'Delete Objects',
        icon: 'fas fa-trash',
        fields: [
            { name: 'bucket', label: 'Bucket Name', type: 'text', help: 'S3 bucket name (leave empty for DBFS paths)' },
            { name: 'object_paths', label: 'Object Paths', type: 'textarea', required: true, help: 'Comma-separated list of object paths to delete' },
            { name: 'dry_run', label: 'Dry Run', type: 'select', options: ['True', 'False'], default: 'False', help: 'Preview what would be deleted without actually deleting' }
        ]
    },
    'copy_objects': {
        name: 'Copy Objects',
        icon: 'fas fa-copy',
        fields: [
            { name: 'bucket', label: 'Source Bucket', type: 'text', help: 'Source S3 bucket name (leave empty for DBFS paths)' },
            { name: 'source_object', label: 'Source Object Path', type: 'text', required: true, help: 'Path of the source object or directory' },
            { name: 'destination_bucket', label: 'Destination Bucket', type: 'text', help: 'Destination S3 bucket name (leave empty for DBFS paths)' },
            { name: 'destination_object', label: 'Destination Object Path', type: 'text', required: true, help: 'Path of the destination object or directory' },
            { name: 'dry_run', label: 'Dry Run', type: 'select', options: ['True', 'False'], default: 'False', help: 'Preview what would be copied without actually copying' }
        ]
    },
    'move_objects': {
        name: 'Move Objects',
        icon: 'fas fa-arrows-alt',
        fields: [
            { name: 'bucket', label: 'Source Bucket', type: 'text', help: 'Source S3 bucket name (leave empty for DBFS paths)' },
            { name: 'source_object', label: 'Source Object Path', type: 'text', required: true, help: 'Path of the source object or directory' },
            { name: 'destination_bucket', label: 'Destination Bucket', type: 'text', help: 'Destination S3 bucket name (leave empty for DBFS paths)' },
            { name: 'destination_object', label: 'Destination Object Path', type: 'text', required: true, help: 'Path of the destination object or directory' },
            { name: 'dry_run', label: 'Dry Run', type: 'select', options: ['True', 'False'], default: 'False', help: 'Preview what would be moved without actually moving' }
        ]
    },
    'request_restore': {
        name: 'Request Restore (S3)',
        icon: 'fas fa-undo',
        fields: [
            { name: 'bucket', label: 'S3 Bucket', type: 'text', required: true, help: 'S3 bucket containing archived objects' },
            { name: 'source_object', label: 'Source Object Path', type: 'text', required: true, help: 'Path of the archived object to restore' },
            { name: 'restore_expiration', label: 'Restore Expiration (days)', type: 'number', required: true, default: '7', help: 'Number of days to keep restored objects available' },
            { name: 'retrieval_tier', label: 'Retrieval Tier', type: 'select', options: ['Expedited', 'Standard', 'Bulk'], default: 'Standard', help: 'Speed and cost tier for restoration' },
            { name: 'dry_run', label: 'Dry Run', type: 'select', options: ['True', 'False'], default: 'False', help: 'Preview what would be restored without actually restoring' }
        ]
    },
    'check_restore_status': {
        name: 'Check Restore Status (S3)',
        icon: 'fas fa-search',
        fields: [
            { name: 'bucket', label: 'S3 Bucket', type: 'text', required: true, help: 'S3 bucket containing archived objects' },
            { name: 'source_object', label: 'Source Object Path', type: 'text', required: true, help: 'Path of the object to check restore status' }
        ]
    },
    'request_restore_to_destination_and_wait': {
        name: 'Request Restore and Copy (S3)',
        icon: 'fas fa-sync-alt',
        fields: [
            { name: 'bucket', label: 'Source S3 Bucket', type: 'text', required: true, help: 'S3 bucket containing archived objects' },
            { name: 'source_object', label: 'Source Object Path', type: 'text', required: true, help: 'Path of the archived object to restore' },
            { name: 'destination_bucket', label: 'Destination S3 Bucket', type: 'text', required: true, help: 'Destination S3 bucket for restored objects' },
            { name: 'destination_object', label: 'Destination Object Path', type: 'text', required: true, help: 'Path of the destination for restored objects' },
            { name: 'restore_expiration', label: 'Restore Expiration (days)', type: 'number', required: true, default: '7', help: 'Number of days to keep restored objects available' },
            { name: 'retrieval_tier', label: 'Retrieval Tier', type: 'select', options: ['Expedited'], default: 'Expedited', help: 'Only Expedited tier supported for this operation' },
            { name: 'dry_run', label: 'Dry Run', type: 'select', options: ['True', 'False'], default: 'False', help: 'Preview what would be restored without actually restoring' }
        ]
    }
};

// ============================================================================
// INITIALIZATION
// ============================================================================
// Set up the application when the DOM is fully loaded

/**
 * Initialize the application on page load
 * Sets up tabs, event listeners, and loads any saved state
 */
document.addEventListener('DOMContentLoaded', function() {
    initializeTabs();
    initializeEventListeners();
    loadFromLocalStorage();
});

// ============================================================================
// TAB MANAGEMENT
// ============================================================================

/**
 * Initialize tab navigation functionality
 * Sets up click handlers for switching between table and file manager tabs
 */
function initializeTabs() {
    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            const tabId = button.getAttribute('data-tab');
            switchTab(tabId);
        });
    });
}

/**
 * Switch to a different tab
 * @param {string} tabId - The ID of the tab to activate ('table-manager' or 'file-manager')
 */
function switchTab(tabId) {
    // Update button active states
    tabButtons.forEach(btn => btn.classList.remove('active'));
    document.querySelector(`[data-tab="${tabId}"]`).classList.add('active');
    
    // Update content visibility
    tabContents.forEach(content => content.classList.remove('active'));
    document.getElementById(tabId).classList.add('active');
    
    // Update application state
    currentTab = tabId;
    updateAddButtonState();
}

// ============================================================================
// EVENT LISTENERS SETUP
// ============================================================================

/**
 * Initialize all event listeners for interactive elements
 * Connects UI actions to their handler functions
 */
function initializeEventListeners() {
    // Operation selection change handlers
    tableOperationSelect.addEventListener('change', handleTableOperationChange);
    fileOperationSelect.addEventListener('change', handleFileOperationChange);
    
    // Button click handlers
    addOperationBtn.addEventListener('click', addCurrentOperation);
    clearOperationsBtn.addEventListener('click', clearAllOperations);
    generateBtn.addEventListener('click', generateJSON);
    copyBtn.addEventListener('click', copyToClipboard);
    downloadBtn.addEventListener('click', downloadJSON);
    formatBtn.addEventListener('click', formatJSON);
    validateBtn.addEventListener('click', validateJSON);
}

// ============================================================================
// DYNAMIC FIELD GENERATION
// ============================================================================

/**
 * Handle table operation selection change
 * Renders the appropriate parameter fields for the selected table operation
 */
function handleTableOperationChange() {
    const operation = tableOperationSelect.value;
    if (operation && TABLE_OPERATIONS[operation]) {
        renderDynamicFields(tableDynamicFields, TABLE_OPERATIONS[operation], 'table');
        updateAddButtonState();
    } else {
        showNoOperationSelected(tableDynamicFields);
        updateAddButtonState();
    }
}

/**
 * Handle file operation selection change
 * Renders the appropriate parameter fields for the selected file operation
 */
function handleFileOperationChange() {
    const operation = fileOperationSelect.value;
    if (operation && FILE_OPERATIONS[operation]) {
        renderDynamicFields(fileDynamicFields, FILE_OPERATIONS[operation], 'file');
        updateAddButtonState();
    } else {
        showNoOperationSelected(fileDynamicFields);
        updateAddButtonState();
    }
}

/**
 * Display a message when no operation is selected
 * @param {HTMLElement} container - The container to display the message in
 */
function showNoOperationSelected(container) {
    container.innerHTML = `
        <div class="no-operation-selected">
            <i class="fas fa-arrow-up"></i>
            <p>Select an operation above to see its configuration options</p>
        </div>
    `;
}

/**
 * Render dynamic parameter fields for the selected operation
 * @param {HTMLElement} container - The container to render fields into
 * @param {Object} operationDef - The operation definition with field specifications
 * @param {string} type - The operation type ('table' or 'file')
 */
function renderDynamicFields(container, operationDef, type) {
    const html = `
        <div class="field-group">
            <h4>
                <i class="${operationDef.icon}"></i>
                ${operationDef.name} Configuration
            </h4>
            ${operationDef.fields.map(field => renderField(field, type)).join('')}
        </div>
    `;
    container.innerHTML = html;
    
    // Attach validation event listeners to all input fields
    container.querySelectorAll('input, select, textarea').forEach(input => {
        input.addEventListener('blur', () => validateField(input));
        input.addEventListener('input', () => clearFieldValidation(input));
    });
}

/**
 * Render a single input field based on its definition
 * @param {Object} field - Field definition with name, type, label, etc.
 * @param {string} type - The operation type for generating unique field IDs
 * @returns {string} HTML string for the field
 */
function renderField(field, type) {
    const fieldId = `${type}-${field.name}`;
    const required = field.required ? 'required' : '';
    const requiredMarker = field.required ? '<span class="field-required">*</span>' : '';
    
    let inputHtml = '';
    
    // Generate appropriate input HTML based on field type
    switch (field.type) {
        case 'text':
        case 'number':
            inputHtml = `<input type="${field.type}" id="${fieldId}" name="${field.name}" ${required} ${field.default ? `value="${field.default}"` : ''}>`;
            break;
        case 'textarea':
            inputHtml = `<textarea id="${fieldId}" name="${field.name}" rows="3" ${required}>${field.default || ''}</textarea>`;
            break;
        case 'select':
            const options = field.options.map(option => 
                `<option value="${option}" ${field.default === option ? 'selected' : ''}>${option}</option>`
            ).join('');
            inputHtml = `<select id="${fieldId}" name="${field.name}" ${required}>${options}</select>`;
            break;
    }
    
    return `
        <div class="field-row">
            <div class="field-item">
                <label for="${fieldId}">
                    ${field.label} ${requiredMarker}
                </label>
                ${inputHtml}
                <div class="field-help">${field.help}</div>
                <div class="validation-message" id="${fieldId}-validation"></div>
            </div>
        </div>
    `;
}

// ============================================================================
// FIELD VALIDATION
// ============================================================================

/**
 * Validate a single input field
 * @param {HTMLInputElement} input - The input element to validate
 * @returns {boolean} True if field is valid, false otherwise
 */
function validateField(input) {
    const validationDiv = document.getElementById(`${input.id}-validation`);
    const isRequired = input.hasAttribute('required');
    const value = input.value.trim();
    
    // Clear previous validation state
    input.classList.remove('valid', 'invalid');
    validationDiv.textContent = '';
    validationDiv.className = 'validation-message';
    
    // Check if required field is empty
    if (isRequired && !value) {
        input.classList.add('invalid');
        validationDiv.textContent = 'This field is required';
        validationDiv.classList.add('error');
        return false;
    }
    
    // Type-specific validation for number fields
    if (value && input.type === 'number') {
        const numValue = parseFloat(value);
        if (isNaN(numValue) || numValue < 0) {
            input.classList.add('invalid');
            validationDiv.textContent = 'Please enter a valid positive number';
            validationDiv.classList.add('error');
            return false;
        }
    }
    
    // Mark field as valid if it has a value
    if (value) {
        input.classList.add('valid');
        validationDiv.textContent = '✓ Valid';
        validationDiv.classList.add('success');
    }
    
    return true;
}

/**
 * Clear validation state from an input field
 * @param {HTMLInputElement} input - The input element to clear validation from
 */
function clearFieldValidation(input) {
    input.classList.remove('valid', 'invalid');
    const validationDiv = document.getElementById(`${input.id}-validation`);
    if (validationDiv) {
        validationDiv.textContent = '';
        validationDiv.className = 'validation-message';
    }
}

// ============================================================================
// OPERATION MANAGEMENT
// ============================================================================

/**
 * Update the enabled/disabled state of the Add Operation button
 * Button is only enabled when an operation is selected
 */
function updateAddButtonState() {
    const currentSelect = currentTab === 'table-manager' ? tableOperationSelect : fileOperationSelect;
    const hasSelection = currentSelect.value !== '';
    addOperationBtn.disabled = !hasSelection;
}

/**
 * Add the currently configured operation to the operations list
 * Validates all fields before adding
 */
function addCurrentOperation() {
    const currentSelect = currentTab === 'table-manager' ? tableOperationSelect : fileOperationSelect;
    const operationKey = currentSelect.value;
    
    if (!operationKey) return;
    
    const operationDef = currentTab === 'table-manager' ? 
        TABLE_OPERATIONS[operationKey] : FILE_OPERATIONS[operationKey];
    
    // Collect and validate field values
    const config = { function: operationKey };
    const container = currentTab === 'table-manager' ? tableDynamicFields : fileDynamicFields;
    let isValid = true;
    
    container.querySelectorAll('input, select, textarea').forEach(input => {
        if (!validateField(input)) {
            isValid = false;
        }
        
        const value = input.value.trim();
        if (value) {
            // Handle different field types and convert values appropriately
            if (input.name === 'object_paths' && value.includes(',')) {
                config[input.name] = value.split(',').map(s => s.trim());
            } else if (input.type === 'number') {
                config[input.name] = parseInt(value, 10);
            } else if (value === 'True') {
                config[input.name] = true;
            } else if (value === 'False') {
                config[input.name] = false;
            } else {
                config[input.name] = value;
            }
        }
    });
    
    // Abort if validation failed
    if (!isValid) {
        showToast('Please fix validation errors before adding the operation', 'error');
        return;
    }
    
    // Create and add operation object
    const operation = {
        id: Date.now(),
        type: currentTab === 'table-manager' ? 'table' : 'file',
        manager: currentTab === 'table-manager' ? 'table' : 'file',
        functionName: operationKey,
        displayName: operationDef.name,
        icon: operationDef.icon,
        config: config
    };
    
    operations.push(operation);
    renderOperationsList();
    updateGenerateButtonState();
    saveToLocalStorage();
    
    showToast(`${operationDef.name} operation added successfully!`, 'success');
}

/**
 * Remove an operation from the operations list
 * @param {number} id - The unique ID of the operation to remove
 */
function removeOperation(id) {
    operations = operations.filter(op => op.id !== id);
    renderOperationsList();
    updateGenerateButtonState();
    saveToLocalStorage();
    showToast('Operation removed', 'success');
}

/**
 * Clear all operations from the list after confirmation
 */
function clearAllOperations() {
    if (operations.length === 0) return;
    
    if (confirm('Are you sure you want to remove all operations?')) {
        operations = [];
        renderOperationsList();
        updateGenerateButtonState();
        saveToLocalStorage();
        showToast('All operations cleared', 'success');
    }
}

/**
 * Render the list of added operations in the UI
 * Shows empty state if no operations exist
 */
function renderOperationsList() {
    if (operations.length === 0) {
        operationsList.innerHTML = `
            <div class="empty-operations">
                <i class="fas fa-clipboard-list"></i>
                <p>No operations added yet. Configure and add operations to build your JSON.</p>
            </div>
        `;
        return;
    }
    
    const html = operations.map(operation => `
        <div class="operation-item ${operation.id === operations[operations.length - 1]?.id ? 'new' : ''}">
            <div class="operation-info">
                <div class="operation-title">
                    <span class="operation-badge badge-${operation.type}">
                        ${operation.type}
                    </span>
                    <i class="${operation.icon}"></i>
                    ${operation.displayName}
                </div>
                <div class="operation-details">
                    Function: <strong>${operation.functionName}</strong> |
                    Parameters: ${Object.keys(operation.config).filter(k => k !== 'function').length}
                </div>
            </div>
            <div class="operation-actions">
                <button class="btn btn-sm btn-remove" onclick="removeOperation(${operation.id})">
                    <i class="fas fa-times"></i>
                    Remove
                </button>
            </div>
        </div>
    `).join('');
    
    operationsList.innerHTML = html;
}

/**
 * Update the enabled/disabled state of the Generate JSON button
 * Button is only enabled when at least one operation exists
 */
function updateGenerateButtonState() {
    generateBtn.disabled = operations.length === 0;
}

// ============================================================================
// JSON GENERATION AND OUTPUT
// ============================================================================

/**
 * Generate JSON configuration from the operations list
 * Creates the final configuration object in Lakehouse Engine format
 */
function generateJSON() {
    if (operations.length === 0) {
        showToast('No operations to generate. Please add at least one operation.', 'error');
        return;
    }
    
    showLoading();
    
    // Use setTimeout to show loading animation
    setTimeout(() => {
        try {
            const config = {
                operations: operations.map(op => ({
                    manager: op.manager,
                    ...op.config
                }))
            };
            
            generatedConfig = config;
            displayJSON(config);
            enableActionButtons();
            showToast('JSON configuration generated successfully!', 'success');
            
        } catch (error) {
            console.error('Generation error:', error);
            showToast('Error generating JSON: ' + error.message, 'error');
        } finally {
            hideLoading();
        }
    }, 500);
}

/**
 * Display formatted JSON in the output area
 * @param {Object} config - The configuration object to display
 */
function displayJSON(config) {
    const formattedJSON = JSON.stringify(config, null, 2);
    jsonOutput.textContent = formattedJSON;
    highlightJSON();
}

/**
 * Apply syntax highlighting to the displayed JSON
 * Colors different JSON elements (keys, strings, numbers, booleans)
 */
function highlightJSON() {
    const content = jsonOutput.textContent;
    const highlighted = content
        .replace(/"([^"]+)":/g, '<span class="json-key">"$1":</span>')
        .replace(/: "([^"]+)"/g, ': <span class="json-string">"$1"</span>')
        .replace(/: (\d+)/g, ': <span class="json-number">$1</span>')
        .replace(/: (true|false)/g, ': <span class="json-boolean">$1</span>')
        .replace(/: null/g, ': <span class="json-null">null</span>');
    
    jsonOutput.innerHTML = highlighted;
}

/**
 * Format the generated JSON with proper indentation
 * Re-formats and re-highlights the JSON output
 */
function formatJSON() {
    if (!generatedConfig) {
        showToast('No JSON to format. Generate configuration first.', 'error');
        return;
    }
    
    try {
        const formatted = JSON.stringify(generatedConfig, null, 2);
        jsonOutput.textContent = formatted;
        highlightJSON();
        showToast('JSON formatted successfully!', 'success');
    } catch (error) {
        showToast('Error formatting JSON: ' + error.message, 'error');
    }
}

/**
 * Validate the generated JSON configuration
 * Checks for required fields and proper structure
 */
function validateJSON() {
    if (!generatedConfig) {
        showValidationResult(false, 'No JSON to validate. Generate configuration first.');
        return;
    }
    
    try {
        const config = generatedConfig;
        const errors = [];
        
        // Check for operations array
        if (!config.operations || !Array.isArray(config.operations)) {
            errors.push('Missing or invalid operations array');
        } else {
            // Validate each operation
            config.operations.forEach((op, index) => {
                if (!op.manager) {
                    errors.push(`Operation ${index + 1}: Missing manager field`);
                }
                if (!op.function) {
                    errors.push(`Operation ${index + 1}: Missing function field`);
                }
            });
        }
        
        // Display validation results
        if (errors.length === 0) {
            showValidationResult(true, `JSON configuration is valid! Contains ${config.operations.length} operation(s).`);
        } else {
            showValidationResult(false, 'Validation errors: ' + errors.join(', '));
        }
    } catch (error) {
        showValidationResult(false, 'Validation error: ' + error.message);
    }
}

/**
 * Display validation results to the user
 * @param {boolean} isValid - Whether the validation passed
 * @param {string} message - The validation message to display
 */
function showValidationResult(isValid, message) {
    validationResult.className = `validation-result ${isValid ? 'valid' : 'invalid'}`;
    validationResult.textContent = isValid ? '✅ ' + message : '❌ ' + message;
}

/**
 * Copy the generated JSON to the clipboard
 * Uses modern Clipboard API with fallback for older browsers
 */
async function copyToClipboard() {
    if (!generatedConfig) {
        showToast('No JSON to copy. Generate configuration first.', 'error');
        return;
    }
    
    try {
        const jsonString = JSON.stringify(generatedConfig, null, 2);
        await navigator.clipboard.writeText(jsonString);
        showToast('JSON copied to clipboard!', 'success');
    } catch (error) {
        // Fallback for older browsers
        const textArea = document.createElement('textarea');
        textArea.value = JSON.stringify(generatedConfig, null, 2);
        document.body.appendChild(textArea);
        textArea.select();
        document.execCommand('copy');
        document.body.removeChild(textArea);
        showToast('JSON copied to clipboard!', 'success');
    }
}

/**
 * Download the generated JSON as a file
 * Creates a timestamped filename and triggers browser download
 */
function downloadJSON() {
    if (!generatedConfig) {
        showToast('No JSON to download. Generate configuration first.', 'error');
        return;
    }
    
    const jsonString = JSON.stringify(generatedConfig, null, 2);
    const blob = new Blob([jsonString], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    // Generate filename with timestamp
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `lakehouse-operations-${timestamp}.json`;
    
    // Trigger download
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    showToast(`Configuration downloaded as ${filename}`, 'success');
}

// ============================================================================
// UI HELPER FUNCTIONS
// ============================================================================

/**
 * Enable the JSON action buttons (copy, download)
 * Called after JSON is successfully generated
 */
function enableActionButtons() {
    copyBtn.disabled = false;
    downloadBtn.disabled = false;
}

/**
 * Show the loading spinner overlay
 */
function showLoading() {
    loading.style.display = 'flex';
}

/**
 * Hide the loading spinner overlay
 */
function hideLoading() {
    loading.style.display = 'none';
}

/**
 * Display a toast notification message
 * @param {string} message - The message to display
 * @param {string} type - The toast type ('success' or 'error')
 */
function showToast(message, type = 'success') {
    toast.textContent = message;
    toast.className = `toast ${type}`;
    toast.classList.add('show');
    
    // Auto-hide after 3 seconds
    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}

// ============================================================================
// LOCAL STORAGE PERSISTENCE
// ============================================================================

/**
 * Save the current operations and state to localStorage
 * Allows users to resume work after page reload
 */
function saveToLocalStorage() {
    const data = {
        operations: operations,
        currentTab: currentTab,
        timestamp: Date.now()
    };
    localStorage.setItem('lakehouse-operations-generator', JSON.stringify(data));
}

/**
 * Load previously saved operations and state from localStorage
 * Only loads data saved within the last 24 hours
 */
function loadFromLocalStorage() {
    try {
        const saved = localStorage.getItem('lakehouse-operations-generator');
        if (saved) {
            const data = JSON.parse(saved);
            
            // Only load if saved within last 24 hours
            if (Date.now() - data.timestamp < 24 * 60 * 60 * 1000) {
                operations = data.operations || [];
                renderOperationsList();
                updateGenerateButtonState();
                
                if (data.currentTab) {
                    switchTab(data.currentTab);
                }
            }
        }
    } catch (error) {
        console.warn('Could not load saved data:', error);
    }
}

// ============================================================================
// KEYBOARD SHORTCUTS
// ============================================================================

/**
 * Handle keyboard shortcuts for common actions
 * - Ctrl/Cmd + G: Generate JSON
 * - Ctrl/Cmd + A: Add operation (when operation selector focused)
 * - Ctrl + Delete: Clear all operations
 */
document.addEventListener('keydown', function(event) {
    // Ctrl+G or Cmd+G - Generate JSON
    if ((event.ctrlKey || event.metaKey) && event.key === 'g') {
        event.preventDefault();
        generateJSON();
    }
    
    // Ctrl+A or Cmd+A when focused on operation selector - Add operation
    if ((event.ctrlKey || event.metaKey) && event.key === 'a' && 
        (event.target === tableOperationSelect || event.target === fileOperationSelect)) {
        event.preventDefault();
        addCurrentOperation();
    }
    
    // Ctrl + Delete - Clear operations
    if (event.key === 'Delete' && event.ctrlKey && operations.length > 0) {
        event.preventDefault();
        clearAllOperations();
    }
});

// ============================================================================
// FINAL INITIALIZATION
// ============================================================================

/**
 * Initialize button states when page loads
 * Ensures all buttons are in the correct enabled/disabled state
 */
document.addEventListener('DOMContentLoaded', function() {
    updateAddButtonState();
    updateGenerateButtonState();
});
