#! /bin/bash

dir="/Users/lmerchant/BCODMO_work/2023/infer_datatypes_osprey/erddap_data_files_temp5"

subdirectories=$(find $dir -type d) # find only subdirectories in dir

for subdir in $subdirectories
do

    if grep -q "dataURL" <<< "$subdir"; then

        in_file=$(find "$subdir" -type f -name "*.tsv")

        echo $in_file

        if test -f "$in_file"; then
            out_file="${in_file/.tsv/.csv}"

            temp_file='temp.tsv'
            temp_file2='temp2.tsv'

            # Replace any commas with ';' and then sub in ',' for '\t'

            # TODO
            # really want to suround contents within tabs that are
            # separated by commas with ""

            # https://stackoverflow.com/questions/53557100/extract-text-between-tabs

            # this regex will find content between tabs with a comma in it
            # (?<=\t)([^\t]+,[^\t]+)(?=\t)

            # I want to use all matches and within those matches surround them
            # with quotes

            # from https://stackoverflow.com/questions/12131134/replace-specific-capture-group-instead-of-entire-regex-in-perl

            # s/(?<=prefix)(your capture)(?=suffix)/$1/

            perl -wnlpe 's/(?<=\t)?([^\t]+,[^\t]+)(?=\t)?/"$1"/g;' $in_file > $temp_file

            # Remove any double quotes since regex was looking for comma delimited
            # content between tabs, and it may already have been quoted
            perl -wnlp -e 's/""/"/g;' $temp_file > $temp_file2

            # Replace tabs with commas
            perl -wnlp -e 's/\t/,/g;' $temp_file > $out_file

            rm $temp_file $temp_file2

        else
            continue
        fi
    fi

done

# ./convert_tsv_to_csv.sh  111.46s user 25.25s system 94% cpu 2:23.99 total


