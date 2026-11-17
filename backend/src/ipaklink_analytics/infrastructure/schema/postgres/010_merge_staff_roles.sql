-- Keep the analyst role code so existing clients and grant references stay valid.
UPDATE analytics_role SET name = 'Staff' WHERE role_code = 'analyst';

UPDATE analytics_access_grant
SET role_code = 'analyst'
WHERE role_code = 'operator';

UPDATE analytics_role SET status = 'disabled' WHERE role_code = 'operator';
