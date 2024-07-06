/*
 * Copyright (c) 2019, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * ----------------------------------------------------------------------------
 *
 * This file implements the LOLWUT command. The command should do something
 * fun and interesting, and should be replaced by a new implementation at
 * each new version of the server.
 *
 * Thanks to Michele Hiki Falcone for the original image that inspired
 * the image, part of his game, Plaguemon.
 *
 * Thanks to the Shhh computer art collective for the help in tuning the
 * output to have a better artistic effect.
 */

#include "server.h"
#include "lolwut.h"

static char ascii_array[] = " .:~=+*#&@";

static inline int juliaSetIteration(float x, float y, float julia_r, float julia_i, int max_iter) {
    for (int i = 0; i < max_iter; i++) {
        float x_new = x * x - y * y + julia_r;
        float y_new = 2 * x * y + julia_i;
        x = x_new;
        y = y_new;
        if (x * x + y * y > 4) return i;
    }
    return max_iter - 1;
}

/* The LOLWUT 8 command:
 *
 * LOLWUT [Real component] [Imaginary component]
 *
 * By default the command uses 80 columns, 40 squares per row per column with
 * a random constant.
 */
void lolwut8Command(client *c) {
    long cols = 80;
    long rows = 40;
    const float julia_r = rand() / (float) RAND_MAX * 2 - 1;
    const float julia_i = rand() / (float) RAND_MAX * 2 - 1;

    /* Parse the optional arguments if any. */
    if (c->argc > 1 && getLongFromObjectOrReply(c, c->argv[1], &cols, NULL) != C_OK) return;
    if (c->argc > 2 && getLongFromObjectOrReply(c, c->argv[2], &rows, NULL) != C_OK) return;

    /* Limits. We want LOLWUT to be always reasonably fast and cheap to execute
     * so we have maximum number of columns, rows, and output resolution. */
    if (cols < 1) cols = 1;
    if (cols > 100) cols = 100;
    if (rows < 1) rows = 1;
    if (rows > 100) rows = 100;

    char *ouput_array = zmalloc(sizeof(char) * cols * rows);
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            float x = (float) 4 * (2 * i) / (2 * rows + 1) - 2.0;
            float y = (float) 4 * (2 * j) / (2 * cols + 1) - 2.0;

            int iterations = juliaSetIteration(x, y, julia_r, julia_i, sizeof(ascii_array) - 1);
            ouput_array[i * cols + j] = ascii_array[iterations % sizeof(ascii_array)];
        }
    }

    sds rendered = sdsempty();
    for (int i = 0; i < rows; i++) {
        rendered = sdscatlen(rendered, &ouput_array[i * cols], cols);
        rendered = sdscatlen(rendered, "\n", 1);
    }

    addReplyVerbatim(c, rendered, sdslen(rendered), "txt");
    sdsfree(rendered);
}
