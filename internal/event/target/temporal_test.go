// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package target

import (
	"testing"

	xnet "github.com/minio/pkg/net"
)

func TestTemporalArgs_Validate(t *testing.T) {
	type fields struct {
		Enable    bool
		Addr      xnet.Host
		TaskQueue string
		Workflow  string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "test1_missing_workflow",
			fields: fields{
				Enable: true,
				Addr: xnet.Host{
					Name:      "",
					Port:      7233,
					IsPortSet: true,
				},
				TaskQueue: "task_queue",
				Workflow:  "",
			},
			wantErr: true,
		},
		{
			name: "test2_missing_task_queue",
			fields: fields{
				Enable: true,
				Addr: xnet.Host{
					Name:      "",
					Port:      7233,
					IsPortSet: true,
				},
				TaskQueue: "",
				Workflow:  "workflow_name",
			},
			wantErr: true,
		},
		{
			name: "test3_disabled",
			fields: fields{
				Enable:    false,
				Addr:      xnet.Host{},
				TaskQueue: "task_queue",
				Workflow:  "workflow_name",
			},
			wantErr: false,
		},
		{
			name: "test4_empty_address",
			fields: fields{
				Enable:    true,
				Addr:      xnet.Host{},
				TaskQueue: "task_queue",
				Workflow:  "workflow_name",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := TemporalArgs{
				Enable:    tt.fields.Enable,
				Addr:      tt.fields.Addr,
				TaskQueue: tt.fields.TaskQueue,
				Workflow:  tt.fields.Workflow,
			}
			if err := tr.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("TemporalArgs.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
