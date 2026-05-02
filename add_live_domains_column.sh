#!/bin/bash
# Add live_domains column to jobs table

VPS_IP="159.203.180.79"
PASSWORD="Yhv5qg2UYvt2TEbU"

echo "Adding live_domains column to jobs table..."

sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no root@$VPS_IP << 'EOF'
sudo -u postgres psql -d scraperdb -c "
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name='jobs' AND column_name='live_domains') THEN
        ALTER TABLE jobs ADD COLUMN live_domains JSONB;
        RAISE NOTICE 'Column live_domains added successfully';
    ELSE
        RAISE NOTICE 'Column live_domains already exists';
    END IF;
END
\$\$;
"

echo "Checking columns in jobs table:"
sudo -u postgres psql -d scraperdb -c "
SELECT column_name FROM information_schema.columns WHERE table_name='jobs' ORDER BY ordinal_position;
"
EOF

echo "Done!"
