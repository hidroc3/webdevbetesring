import { Test, TestingModule } from '@nestjs/testing';
import { ReportDetailsService } from './report-details.service';

describe('ReportDetailsService', () => {
  let service: ReportDetailsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [ReportDetailsService],
    }).compile();

    service = module.get<ReportDetailsService>(ReportDetailsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
