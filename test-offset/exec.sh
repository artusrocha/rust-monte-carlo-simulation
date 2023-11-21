echo "100k" > test.tsv
./target/release/test-offset 2 >> test.tsv
echo "" >> test.tsv

echo "500k" >> test.tsv
./target/release/test-offset 3 >> test.tsv
echo "" >> test.tsv

echo "2mi" >> test.tsv
./target/release/test-offset 1 >> test.tsv
echo "" >> test.tsv
