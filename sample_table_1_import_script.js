const fs = require('fs');
const path = require('path');
const { Client } = require('pg');
const csv = require('csv-parser');
require('dotenv').config({ path: '.env' })

// Use this to import CSV files from a directory into a PostgreSQL database
// Replace the values below with your own values
const directoryPath = './'+ process.env.DIRECTORY_PATH;
const connectionString = 'postgresql://' + process.env.DB_USER + ":" + process.env.DB_USER_PASSWORD + "@" + process.env.DB_HOST + ":" + process.env.DB_PORT + "/" + process.env.DB_NAME;
console.log('Connection string: ', connectionString);
console.log('Directory path: ', directoryPath);

// Create a new PostgreSQL client
const client = new Client({
    connectionString: connectionString,
});

// Function to import CSV files into the database
// Replace 'etc.sample_tbl_1' with the name of the table you want to import the data into
// Replace '(column_name1, column_name2, ...)' with the column names in your table
// Replace '$1, $2, ...' with the corresponding values from the CSV file by row.column_name
async function importCSV(filePath) {
    await client.connect();
    console.log('Connection connected: ', client.host, client.database, client.port, client.user, client.password);
    const results = [];
    // Read the CSV file and insert the data into the database
    fs.createReadStream(filePath)
        .pipe(csv({ skipLines: 1, headers: ['external_id', 'item_id', 'display_name', 'item_name', 
            'item_number_name', 'vendor_name_code', 'sales_description', 'sales_packaging_unit', 'sale_qty_per_pack_unit', 
            'item_color', 'item_size', 'pcs_in_box', 'sqft_by_pcs_sheet', 'sqft_by_box', 'price_by_UOM', 
            'units_type', 'stock_unit', 'purchase_unit', 'sales_units', 'parent', 'subsidiary', 
            'include_children', 'department', "class", "location", 'costing_method', "cost", 
            'purchase_description', 'stock_description', 'match_bill_to_receipt', 'use_bins', 
            'supply_replenishment_method', 'alternate_demand_source_item', 
            'auto_preferred_stock_level', 'reorder_multiple', 'is_special_order_item', 
            'auto_reorder_point', 'auto_lead_time', 'lead_time', 'safety_stock_level', 
            'safety_stock_level_days', 'transfer_price', 'preferred_location', 
            'item_bin_number1', 'preferred_per_location', 'vendor1_name', 'vendor1_subsidiary', 
            'vendor1_preferred', 'vendor1_purchase_price', 'vendor1_schedule', 'vendor1_code', 
            'vendor2_name', 'vendor2_subsidiary', 'vendor2_preferred', 
            'vendor2_purchase_price', 'vendor2_code', 'item_location_line1_location', 
            'item_location_line1_default_return_cost', 'item_location_line1_preferred_stock_level', 
            'item_location_line1_reorder_print', 'item_location_line1_lot_numbers', 
            'item_location_line1_lot_sizing_numbers', 'cost_estimate_type', 'cost_estimate', 
            'minimum_quantity', 'enforce_qty_internally', 'item_price_line1_item_price_type_ref', 
            'item_price_line1_item_price', 'item_price_line1_quantity_pricing', 'cogs_account', 'income_account', 'asset_account', 
            'bill_price_variance_acct', 'bill_qty_variance_acct', 'bill_exch_variance_acct', 
            'cust_return_variance_account', 'vend_return_variance_account', 'tax_schedule'] }))
        .on('data', (data) => results.push(data))
        .on('end', async () => {
            const date = new Date().toISOString();
            for (const row of results) {
                row.filename = filePath.replace('csv\\','');
                row.date_upload = date;
                row.uploaded_by = process.env.UPLOADED_BY;
                const query = `INSERT INTO etc.sample_tbl_1(filename, date_upload, uploaded_by,
                    external_id, item_id, display_name, item_name, item_number_name, 
                    vendor_name_code, sales_description, sales_packaging_unit, sale_qty_per_pack_unit, 
                    item_color, item_size, pcs_in_box, sqft_by_pcs_sheet, sqft_by_box, price_by_UOM, 
                    units_type, stock_unit, purchase_unit, sales_units, parent, subsidiary, 
                    include_children, department, "class", "location", costing_method, "cost", 
                    purchase_description, stock_description, match_bill_to_receipt, use_bins, 
                    supply_replenishment_method, alternate_demand_source_item, 
                    auto_preferred_stock_level, reorder_multiple, is_special_order_item, 
                    auto_reorder_point, auto_lead_time, lead_time, safety_stock_level, 
                    safety_stock_level_days, transfer_price, preferred_location, 
                    item_bin_number1, preferred_per_location, vendor1_name, vendor1_subsidiary, 
                    vendor1_preferred, vendor1_purchase_price, vendor1_schedule, vendor1_code, 
                    vendor2_name, vendor2_subsidiary, vendor2_preferred, 
                    vendor2_purchase_price, vendor2_code, item_location_line1_location, 
                    item_location_line1_default_return_cost, item_location_line1_preferred_stock_level, 
                    item_location_line1_reorder_print, item_location_line1_lot_numbers, 
                    item_location_line1_lot_sizing_numbers, cost_estimate_type, cost_estimate, 
                    minimum_quantity, enforce_qty_internally, item_price_line1_item_price_type_ref, 
                    item_price_line1_item_price, item_price_line1_quantity_pricing, cogs_account, income_account, asset_account, 
                    bill_price_variance_acct, bill_qty_variance_acct, bill_exch_variance_acct, 
                    cust_return_variance_account, vend_return_variance_account, tax_schedule) 
                    VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, $75, $76, $77, $78, $79, $80, $81)`;
                const values = [row.filename, row.date_upload, row.uploaded_by, row.external_id, 
                    row.item_id, row.display_name, row.item_name, row.item_number_name, row.vendor_name_code, 
                    row.sales_description, row.sales_packaging_unit, row.sale_qty_per_pack_unit, 
                    row.item_color, row.item_size, row.pcs_in_box, row.sqft_by_pcs_sheet, row.sqft_by_box, 
                    row.price_by_UOM, row.units_type, row.stock_unit, row.purchase_unit, row.sales_units, 
                    row.parent, row.subsidiary, row.include_children, row.department, row["class"], 
                    row["location"], row.costing_method, row["cost"], row.purchase_description, row.stock_description, 
                    row.match_bill_to_receipt, row.use_bins, row.supply_replenishment_method, 
                    row.alternate_demand_source_item, row.auto_preferred_stock_level, row.reorder_multiple, 
                    row.is_special_order_item, row.auto_reorder_point, row.auto_lead_time, row.lead_time, 
                    row.safety_stock_level, row.safety_stock_level_days, row.transfer_price, 
                    row.preferred_location, row.item_bin_number1, row.preferred_per_location, row.vendor1_name, 
                    row.vendor1_subsidiary, row.vendor1_preferred, row.vendor1_purchase_price, row.vendor1_schedule, 
                    row.vendor1_code, row.vendor2_name, row.vendor2_subsidiary, row.vendor2_preferred, 
                    row.vendor2_purchase_price, row.vendor2_code, row.item_location_line1_location, 
                    row.item_location_line1_default_return_cost, row.item_location_line1_preferred_stock_level, 
                    row.item_location_line1_reorder_print, row.item_location_line1_lot_numbers, row.item_location_line1_lot_sizing_numbers,
                    row.cost_estimate_type, row.cost_estimate, row.minimum_quantity, row.enforce_qty_internally, 
                    row.item_price_line1_item_price_type_ref, row.item_price_line1_item_price, row.item_price_line1_quantity_pricing, 
                    row.cogs_account, row.income_account, row.asset_account, row.bill_price_variance_acct, 
                    row.bill_qty_variance_acct, row.bill_exch_variance_acct, row.cust_return_variance_account, 
                    row.vend_return_variance_account, row.tax_schedule];
                const resultInsert = await client.query(query, values);
                console.log('Row inserted: ', resultInsert);
            }
            await client.end();
            console.log('CSV file successfully processed and data inserted into the database');
        });
}

// Read all files in the directory and import them into the database
// Only files with a .csv extension will be processed
fs.readdir(directoryPath, (err, files) => {
    if (err) {
        return console.log('Unable to scan directory: ' + err);
    }
    files.forEach((file) => {
        if (path.extname(file) === '.csv') {
            console.log('Processing file: ', directoryPath+'/'+file);
            importCSV(path.join(directoryPath, file));
        }
    });
});
