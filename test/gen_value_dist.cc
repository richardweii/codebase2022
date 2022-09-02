#include <cstdio>

int main() {
  int seq[1024];
  int base = 0;

  // 70%
  for (int i = 0; i < 91 * 2; i++) {
    seq[base + i] = 5;
  }
  base += 91 * 2;

  for (int i = 0; i < 90 * 2; i++) {
    seq[base + i] = 6;
  }
  base += 90 * 2;

  for (int i = 0; i < 90 * 2; i++) {
    seq[base + i] = 7;
  }
  base += 90 * 2;

  for (int i = 0; i < 90 * 2; i++) {
    seq[base + i] = 8;
  }
  base += 90 * 2;

  // 20%
  for (int i = 0; i < 40 * 2; i++) {
    seq[base + i] = 9;
  }
  base += 40 * 2;

  for (int i = 0; i < 20 * 2; i++) {
    seq[base + i] = 10;
  }
  base += 20 * 2;

  for (int i = 0; i < 10 * 2; i++) {
    seq[base + i] = 11;
  }
  base += 10 * 2;

  for (int i = 0; i < 10 * 2; i++) {
    seq[base + i] = 12;
  }
  base += 10 * 2;

  for (int i = 0; i < 5 * 2; i++) {
    seq[base + i] = 13;
  }
  base += 5 * 2;

  for (int i = 0; i < 5 * 2; i++) {
    seq[base + i] = 14;
  }
  base += 5 * 2;

  for (int i = 0; i < 5 * 2; i++) {
    seq[base + i] = 15;
  }
  base += 5 * 2;

  for (int i = 0; i < 5 * 2; i++) {
    seq[base + i] = 16;
  }
  base += 5 * 2;

  //%10
  for (int i = 0; i < 19; i++) {
    seq[base + i] = 17;
  }
  base += 19;

  for (int i = 0; i < 10; i++) {
    seq[base + i] = 18;
  }
  base += 10;

  for (int i = 0; i < 10; i++) {
    seq[base + i] = 19;
  }
  base += 10;

  for (int i = 0; i < 10; i++) {
    seq[base + i] = 20;
  }
  base += 10;

  for (int i = 0; i < 10; i++) {
    seq[base + i] = 21;
  }
  base += 10;

  for (int i = 0; i < 43; i++) {
    seq[base + i] = 22 + i;
  }

  printf("{%d", seq[0]);
  for (int i = 1; i < 1024; i++) {
    printf(",%d", seq[i]);
  }
  printf("}");

  int sum = 0;
  for (int i = 0; i < 1024; i++) {
    sum += seq[i];
  }
  printf("\navg %f\n", sum * 16.0 / 1024);

  int seq1[64];
  int base1 = 0;

  for (int i = 0; i < 12; i++) {
    seq1[base1 + i] = 5;
  }
  base1 += 12;

  for (int i = 0; i < 11; i++) {
    seq1[base1 + i] = 6;
  }
  base1 += 11;

  for (int i = 0; i < 11; i++) {
    seq1[base1 + i] = 7;
  }
  base1 += 11;

  for (int i = 0; i < 11; i++) {
    seq1[base1 + i] = 8;
  }
  base1 += 11;

  for (int i = 0; i < 4; i++) {
    seq1[base1 + i] = 9;
  }
  base1 += 4;

  for (int i = 0; i < 3; i++) {
    seq1[base1 + i] = 10;
  }
  base1 += 3;

  for (int i = 0; i < 2; i++) {
    seq1[base1 + i] = 11;
  }
  base1 += 2;

  for (int i = 0; i < 2; i++) {
    seq1[base1 + i] = 12;
  }
  base1 += 2;

  for (int i = 0; i < 2; i++) {
    seq1[base1 + i] = 13;
  }
  base1 += 2;

  for (int i = 0; i < 2; i++) {
    seq1[base1 + i] = 14;
  }
  base1 += 2;

  for (int i = 0; i < 2; i++) {
    seq1[base1 + i] = 15;
  }
  base1 += 2;

  for (int i = 0; i < 2; i++) {
    seq1[base1 + i] = 16;
  }
  base1 += 2;

  printf("{%d", seq1[0]);
  for (int i = 1; i < 64; i++) {
    printf(",%d", seq1[i]);
  }
  printf("}");

  int sum1 = 0;
  for (int i = 0; i < 64; i++) {
    sum1 += seq1[i];
  }
  printf("\navg %f\n", sum1 * 16.0 / 64);
}
