f=data/2015_07_22_mktplace_shop_web_log_sample.log
if [ ! -f $f ]; then 
    echo "unzipping $f.gz"
    gunzip -k $f; 
else 
    echo "skipping $f"
fi
