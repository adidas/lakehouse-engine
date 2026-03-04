# Table and File Manager Operations Generator

Generate JSON configurations for TableManager and FileManager operations with an interactive form.

<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
<link rel="stylesheet" href="../managerhelper/styles-mkdocs.css">
<link rel="stylesheet" href="../managerhelper/operations-styles-mkdocs.css">

<div markdown="0" class="managerhelper-wrapper">

    <!-- Navigation Tabs -->
    <nav class="tabs">
        <button class="tab-button active" data-tab="table-manager">
            <i class="fas fa-table"></i>
            Table Manager
        </button>
        <button class="tab-button" data-tab="file-manager">
            <i class="fas fa-folder"></i>
            File Manager
        </button>
    </nav>

    <!-- Operations Container -->
    <main class="operations-container">
        <!-- Table Manager Tab -->
        <div id="table-manager" class="tab-content active">
            <div class="section">
                <h3><i class="fas fa-table"></i> Table Manager Operations</h3>
                
                <div class="operation-selector">
                    <label for="table-operation-select">Select Operation:</label>
                    <select id="table-operation-select" class="form-control">
                        <option value="">Choose an operation...</option>
                        <option value="compute_table_statistics">Compute Table Statistics</option>
                        <option value="create_table">Create Table</option>
                        <option value="create_tables">Create Multiple Tables</option>
                        <option value="create_view">Create View</option>
                        <option value="drop_table">Drop Table</option>
                        <option value="drop_view">Drop View</option>
                        <option value="execute_sql">Execute SQL</option>
                        <option value="truncate">Truncate Table</option>
                        <option value="vacuum">Vacuum Table</option>
                        <option value="describe">Describe Table</option>
                        <option value="optimize">Optimize Table</option>
                        <option value="show_tbl_properties">Show Table Properties</option>
                        <option value="get_tbl_pk">Get Table Primary Key</option>
                        <option value="repair_table">Repair Table</option>
                        <option value="delete_where">Delete Where</option>
                    </select>
                </div>

                <!-- Dynamic form fields will be inserted here -->
                <div id="table-dynamic-fields" class="dynamic-fields">
                    <div class="no-operation-selected">
                        <i class="fas fa-arrow-up"></i>
                        <p>Select an operation above to see its configuration options</p>
                    </div>
                </div>
            </div>
        </div>

        <!-- File Manager Tab -->
        <div id="file-manager" class="tab-content">
            <div class="section">
                <h2><i class="fas fa-folder"></i> File Manager Operations</h2>
                
                <div class="operation-selector">
                    <label for="file-operation-select">Select Operation:</label>
                    <select id="file-operation-select" class="form-control">
                        <option value="">Choose an operation...</option>
                        <option value="delete_objects">Delete Objects</option>
                        <option value="copy_objects">Copy Objects</option>
                        <option value="move_objects">Move Objects</option>
                        <option value="request_restore">Request Restore (S3)</option>
                        <option value="check_restore_status">Check Restore Status (S3)</option>
                        <option value="request_restore_to_destination_and_wait">Request Restore and Copy (S3)</option>
                    </select>
                </div>

                <!-- Dynamic form fields will be inserted here -->
                <div id="file-dynamic-fields" class="dynamic-fields">
                    <div class="no-operation-selected">
                        <i class="fas fa-arrow-up"></i>
                        <p>Select an operation above to see its configuration options</p>
                    </div>
                </div>
            </div>
        </div>
    </main>

    <!-- Operations List -->
    <div class="operations-list-container">
        <div class="operations-header">
            <h4><i class="fas fa-list"></i> Operations Queue</h4>
            <div class="operations-actions">
                <button id="add-operation" class="btn btn-primary" disabled>
                    <i class="fas fa-plus"></i>
                    Add Operation
                </button>
                <button id="clear-operations" class="btn btn-outline">
                    <i class="fas fa-trash"></i>
                    Clear All
                </button>
            </div>
        </div>
        
        <div id="operations-list" class="operations-list">
            <div class="empty-operations">
                <i class="fas fa-clipboard-list"></i>
                <p>No operations added yet. Configure and add operations to build your JSON.</p>
            </div>
        </div>
    </div>

    <!-- Actions -->
    <div class="actions">
        <button id="generate-json" class="btn btn-primary" disabled>
            <i class="fas fa-code"></i>
            Generate JSON
        </button>
        <button id="copy-json" class="btn btn-secondary" disabled>
            <i class="fas fa-copy"></i>
            Copy to Clipboard
        </button>
        <button id="download-json" class="btn btn-secondary" disabled>
            <i class="fas fa-download"></i>
            Download JSON
        </button>
    </div>

    <!-- JSON Output -->
    <div class="output-container">
        <div class="output-header">
            <h4><i class="fas fa-file-code"></i> Generated JSON Configuration</h4>
            <div class="output-actions">
                <button id="format-json" class="btn btn-sm">
                    <i class="fas fa-indent"></i>
                    Format
                </button>
                <button id="validate-json" class="btn btn-sm">
                    <i class="fas fa-check"></i>
                    Validate
                </button>
            </div>
        </div>
        <pre id="json-output" class="json-output"></pre>
        <div id="validation-result" class="validation-result"></div>
    </div>

    <!-- Loading Spinner -->
    <div id="loading" class="loading" style="display: none;">
        <div class="spinner"></div>
        <p>Generating configuration...</p>
    </div>

    <!-- Success Toast -->
    <div id="toast" class="toast"></div>
</div>

<script src="../managerhelper/operations-script.js"></script>
