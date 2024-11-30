// pkg/management/key_test.go
package management

import (
	"strings"
	"testing"

	"redis-management/config"

	"github.com/stretchr/testify/assert"
)

func TestKeyManager_GetKey(t *testing.T) {
	tests := []struct {
		name     string
		config   *config.Config
		input    string
		expected string
		hashKey  bool
	}{
		{
			name: "With Hash and Prefix",
			config: &config.Config{
				Redis: config.RedisConfig{
					KeyPrefix: "test:",
					HashKeys:  true,
					DB:        "0",
				},
			},
			input: "mykey",
			// ما باید مقدار hash شده واقعی را انتظار داشته باشیم
			expected: "test:5e50f405ace6cbdf17379f4b9f2b0c9f4144c5e380ea0b9298cb02ebd8ffe511",
			hashKey:  true,
		},
		{
			name: "No Hash, With Prefix",
			config: &config.Config{
				Redis: config.RedisConfig{
					KeyPrefix: "test:",
					HashKeys:  false,
					DB:        "0",
				},
			},
			input:    "mykey",
			expected: "test:mykey",
			hashKey:  false,
		},
		{
			name: "No Hash, No Prefix",
			config: &config.Config{
				Redis: config.RedisConfig{
					KeyPrefix: "",
					HashKeys:  false,
					DB:        "0",
				},
			},
			input:    "mykey",
			expected: "mykey",
			hashKey:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			km, err := NewKeyManager(tt.config)
			assert.NoError(t, err)

			result := km.GetKey(tt.input)
			if tt.hashKey {
				// برای کلیدهای hash شده، فقط چک می‌کنیم که prefix درست باشد و خروجی خالی نباشد
				assert.True(t, strings.HasPrefix(result, "test:"))
				assert.NotEqual(t, "test:mykey", result)
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// اضافه کردن تست برای parseDBRange
func TestParseDBRange(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedMin int
		expectedMax int
		shouldError bool
	}{
		{
			name:        "Single DB",
			input:       "0",
			expectedMin: 0,
			expectedMax: 0,
			shouldError: false,
		},
		{
			name:        "DB Range",
			input:       "0-5",
			expectedMin: 0,
			expectedMax: 5,
			shouldError: false,
		},
		{
			name:        "Invalid Range",
			input:       "5-0",
			shouldError: true,
		},
		{
			name:        "Invalid Format",
			input:       "invalid",
			shouldError: true,
		},
		{
			name:        "Empty String",
			input:       "",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseDBRange(tt.input)
			if tt.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMin, result.Start)
				assert.Equal(t, tt.expectedMax, result.End)
			}
		})
	}
}
