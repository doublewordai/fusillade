-- Remove triggers and functions for output/error file size tracking

DROP TRIGGER IF EXISTS update_error_file_size_on_fail ON requests;
DROP FUNCTION IF EXISTS update_error_file_size();

DROP TRIGGER IF EXISTS update_output_file_size_on_complete ON requests;
DROP FUNCTION IF EXISTS update_output_file_size();